package net.wicp.tams.duckula.kafka.consumer;

import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.apiext.OSinfo;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.common.metrics.utility.TsLogger;
import net.wicp.tams.common.others.constant.SeekPosition;
import net.wicp.tams.common.others.kafka.KafkaConsumerGroup;
import net.wicp.tams.common.others.kafka.KafkaConsumerGroupB;
import net.wicp.tams.duckula.common.ConfUtil;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.Consumer;
import net.wicp.tams.duckula.common.beans.SenderConsumerEnum;
import net.wicp.tams.duckula.common.beans.Task;
import net.wicp.tams.duckula.common.constant.CommandType;
import net.wicp.tams.duckula.common.constant.MiddlewareType;
import net.wicp.tams.duckula.kafka.consumer.impl.KafkaConsumer;
import net.wicp.tams.duckula.kafka.consumer.jmx.ConsumerControl;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import javax.management.*;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MainConsumer {
    static {
        System.setProperty("Log4jContextSelector", "org.apache.logging.log4j.core.async.AsyncLoggerContextSelector");
    }

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(MainConsumer.class);
    public static ConsumerGroup metric;

    @SuppressWarnings("unchecked")
    public void init(String[] args) throws SQLException {
        Thread.currentThread().setName("Consumer-main");
        if (ArrayUtils.isEmpty(args)) {
            System.err.println("----未传入taskid，不能启动task----");
            log.error("----未传入taskid，不能启动task----");
            return;
        }
        consumerId = args[0];
        log.info("----------------------加载配置文件-------------------------------------");
        CommandType.consumer.setCommonProps();
        log.info("----------------------分布式锁-------------------------------------");
        InterProcessMutex lock = ZkUtil.lockConsumerPath(consumerId);
        try {
            if (!lock.acquire(60, TimeUnit.SECONDS)) {
                List<String> ips = ZkClient.getInst().lockValueList(lock);
                log.error("已有服务[{}]在运行中,无法获得锁.", CollectionUtil.listJoin(ips, ","));
                LoggerUtil.exit(JvmStatus.s9);
            }
        } catch (Exception e1) {
            log.error("获取锁异常", e1);
            LoggerUtil.exit(JvmStatus.s9);
        }

        log.info("----------------------启动jmx-------------------------------------");
        try {
            initMbean(lock);// 启动jxmx
        } catch (Exception e) {
            log.error("启动jmx错误", e);
            LoggerUtil.exit(JvmStatus.s15);
        }
        log.info("----------------------配置metrix-------------------------------------");
        System.setProperty(TsLogger.ENV_FILE_NAME, "consumer_" + consumerId);
        System.setProperty(TsLogger.ENV_FILE_ROOT, String.format("%s/logs/metrics", System.getenv("DUCKULA_DATA")));
        metric = new ConsumerGroup(consumerId);
        log.info("----------------------导入配置-------------------------------------");
        Consumer consumer = ZkUtil.buidlConsumer(consumerId);
        addShutdownHook();
        addTimer();
        addTimerForLock(consumerId);
        Task task = ZkUtil.buidlTask(consumer.getTaskOnlineId());
        Properties kafkaProp = ConfUtil.configMiddleware(MiddlewareType.kafka, task.getMiddlewareInst());
        Conf.overProp(kafkaProp);
        Properties props = new Properties();
        props.put("common.jdbc.datasource.default.host", task.getIp());
        if (StringUtil.isNotNull(task.getDefaultDb())) {
            props.put("common.jdbc.datasource.default.defaultdb", task.getDefaultDb());
        } else {
            props.put("common.jdbc.datasource.default.defaultdb", "null");
        }
        if (task.getIsSsh() != null && task.getIsSsh() == YesOrNo.yes) {
            props.put("common.jdbc.ssh.enable", "true");
        } else {
            props.put("common.jdbc.ssh.enable", "false");
        }
        props.put("common.jdbc.datasource.default.port", String.valueOf(task.getPort()));
        props.put("common.jdbc.datasource.default.username", task.getUser());
        props.put("common.jdbc.datasource.default.password", task.getPwd());
        // 默认不创建连接
        props.put("common.jdbc.datasource.default.initialSize", 0);
        props.put("common.jdbc.datasource.default.maxActive", 32);// 最多32分区，32个线程

        // 设置consumer配置
        props.put("common.others.kafka.consumer.batch.num", consumer.getBatchNum());
        props.put("common.others.kafka.consumer.batch.timeout", consumer.getBatchTimeout());

        Conf.overProp(props);
        log.info("----------------------启动consumer-------------------------------------");

        // 薛健修改:目前只有一个Consumer - KafkaConsumer
        // DEBUG:设置为es7
//        consumer.setSenderConsumerEnum(SenderConsumerEnum.es7);
        KafkaConsumer<byte[]> doConsumer = new KafkaConsumer<>(consumer);

        String groupId = StringUtil.isNull(consumer.getGroupId()) ? Conf.get("common.others.kafka.consumer.group.id")
                : consumer.getGroupId();
        KafkaConsumerGroup<byte[]> group = new KafkaConsumerGroupB(groupId, consumer.getTopic(), doConsumer, 1);
        if (consumer.getStartPosition() != null && consumer.getStartPosition() >= -1) {
            if (consumer.getStartPosition() == 0) {
                group.seekPotion(SeekPosition.begin, null);
            } else if (consumer.getStartPosition() == -1) {// -1为end开始
                group.seekPotion(SeekPosition.end, null);
            } else {
                group.seekPotion(SeekPosition.user, consumer.getStartPosition());
            }
        }
        group.start();
    }

    private String consumerId;

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("----------------------执行关闭进程 钩子开始-------------------------------------");
                // DisruptorManager.getInst().stop(); // 为什么hold住？
                updateLastId();
                log.info("----------------------执行关闭进程 钩子完成-------------------------------------");
            }
        });
    }

    private void addTimer() {
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
        service.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                updateLastId();
            }
        }, 10, 3, TimeUnit.SECONDS);
    }

    private static void addTimerForLock(String consumerId) {
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
        service.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                InterProcessMutex lock = null;
                try {
                    lock = ZkUtil.lockConsumerPath(consumerId);
                    if (!lock.acquire(15, TimeUnit.SECONDS)) {// 只等半分钟就好了
                        List<String> ips = ZkClient.getInst().lockValueList(lock);
                        if (!ips.contains(OSinfo.findIpAddressTrue())) {
                            log.error("此任务的分布式锁已丢失，已获得锁ip地址.", CollectionUtil.listJoin(ips, ","));
                            LoggerUtil.exit(JvmStatus.s9);
                        }
                    }
                } catch (Exception e1) {
                    log.error("获取锁异常", e1);
                    LoggerUtil.exit(JvmStatus.s9);
                }
            }
        }, 10, 20, TimeUnit.SECONDS);
    }

    private void updateLastId() {
        // System.out.println("aaaa");
    }

    public static void main(String[] args) throws IOException, SQLException {
        MainConsumer main = new MainConsumer();
        main.init(args);
        System.in.read();
    }

    private static void initMbean(InterProcessMutex lock) throws InstanceAlreadyExistsException,
            MBeanRegistrationException, NotCompliantMBeanException, MalformedObjectNameException {
        ConsumerControl control = new ConsumerControl();
        control.setLock(lock);
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        mbs.registerMBean(control,
                new ObjectName("net.wicp.tams.duckula:service=Consumer,name=DuckulaConsumer"));// + Conf.get("duckula.consumer.mbean.beanname")
        log.info("----------------------MBean注册成功-------------------------------------");
    }

}
