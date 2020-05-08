package net.wicp.tams.duckula.kafka.consumer.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.alibaba.fastjson.JSONObject;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.protobuf.InvalidProtocolBufferException;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.Plugin;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.TarUtil;
import net.wicp.tams.common.apiext.TimeAssist;
import net.wicp.tams.common.apiext.jdbc.JdbcAssit;
import net.wicp.tams.common.apiext.jdbc.MySqlAssit;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.common.exception.ExceptAll;
import net.wicp.tams.common.exception.ProjectExceptionRuntime;
import net.wicp.tams.common.jdbc.DruidAssit;
import net.wicp.tams.common.others.kafka.IConsumer;
import net.wicp.tams.duckula.client.DuckulaAssit;
import net.wicp.tams.duckula.client.DuckulaIdempotentAssit;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEvent;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEvent.Builder;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEventIdempotent;
import net.wicp.tams.duckula.client.Protobuf3.IdempotentEle;
import net.wicp.tams.duckula.client.Protobuf3.OptType;
import net.wicp.tams.duckula.common.ConfUtil;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.Consumer;
import net.wicp.tams.duckula.common.beans.DbInstance;
import net.wicp.tams.duckula.common.beans.SenderConsumerEnum;
import net.wicp.tams.duckula.common.beans.Task;
import net.wicp.tams.duckula.common.constant.PluginType;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.kafka.consumer.MainConsumer;
import net.wicp.tams.duckula.plugin.PluginAssit;
import net.wicp.tams.duckula.plugin.RuleManager;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.constant.RuleItem;
import net.wicp.tams.duckula.plugin.receiver.consumer.ConsumerSenderAbs;
import net.wicp.tams.duckula.plugin.receiver.consumer.IBusiConsumer;
import net.wicp.tams.duckula.plugin.receiver.consumer.IConsumerSender;

@Slf4j
public class KafkaConsumer<T> implements IConsumer<byte[]> {
    protected final Consumer consumer;
    // 构造Consumer时传入Sender
    protected final IConsumerSender<T> sender;
    private final RuleManager ruleManager;
    protected final IBusiConsumer<T> busiEs;
    //protected Connection connection = DruidAssit.getConnection();// TODO 每线程一个连接
    protected Map<String, String[]> primarysMap = new HashMap<>();
    protected Map<String, String[]> colsMap = new HashMap<>();

    protected static final org.slf4j.Logger errorlog = org.slf4j.LoggerFactory.getLogger("errorBinlog");// 需要跳过的错误数据。

    protected final boolean isIde;

    protected final JSONObject configObject;

    public static File rootDir;

    static {
        rootDir = new File(System.getenv("DUCKULA_DATA"));
    }

    @SuppressWarnings("unchecked")
    public KafkaConsumer(Consumer consumer) {
        this.consumer = consumer;
        this.sender = loadSender();
        ruleManager = new RuleManager(this.consumer.getRuleList());
        this.configObject = ConfUtil.getConfigGlobal();
        if (StringUtil.isNotNull(consumer.getBusiPlugin())) {
            String plugDir = consumer.getBusiPlugin().replace(".tar", "");
            String plugDirSys = IOUtil.mergeFolderAndFilePath(PluginType.consumer.getPluginDir(false), plugDir);
            try {
                File dir = new File(plugDirSys);
                if (!dir.exists() || !dir.isDirectory()) {// 是否存在插件目录
                    AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(this.configObject.getString("region"))
                            .withCredentials(new AWSStaticCredentialsProvider(
                                    new BasicAWSCredentials(this.configObject.getString("accessKey"),
                                            this.configObject.getString("secretKey"))))
                            .build();
                    String key = IOUtil.mergeFolderAndFilePath(PluginType.consumer.getPluginDirKey(),
                            consumer.getBusiPlugin());
                    String tarFileName = plugDirSys + ".tar";
                    getObjectForFile(s3, this.configObject.getString("bucketName"), key, tarFileName);
                    // 解压
                    TarUtil.decompress(tarFileName);
                }
            } catch (Exception e) {
                log.error("下载业务插件及解压失败", e);
                LoggerUtil.exit(JvmStatus.s9);
                throw new RuntimeException("下载业务插件及解压失败");
            }

            try {
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                Plugin plugin = PluginAssit.newPlugin(plugDirSys,
                        "net.wicp.tams.duckula.plugin.receiver.consumer.IBusiConsumer", classLoader,
                        "net.wicp.tams.duckula.plugin.receiver.consumer.IBusiConsumer",
                        "net.wicp.tams.duckula.plugin.receiver.ReceiveAbs");
                Thread.currentThread().setContextClassLoader(plugin.getLoad().getClassLoader());// 需要加载前设置好classload
                this.busiEs = (IBusiConsumer<T>) plugin.newObject();
                log.info("the busi plugin is sucess");
            } catch (Throwable e) {
                log.error("组装业务插件失败", e);
                LoggerUtil.exit(JvmStatus.s9);
                throw new RuntimeException("组装业务插件失败");
            }
        } else {
            busiEs = null;
        }
        Task task = ZkUtil.buidlTask(consumer.getTaskOnlineId());
        isIde = task.getSenderEnum().isIdempotent();
        if (consumer.getMiddlewareType() != null && StringUtil.isNotNull(consumer.getMiddlewareInst())) {// 中间件集群相关配置
            Properties props = ConfUtil.configMiddleware(consumer.getMiddlewareType(), consumer.getMiddlewareInst());
            Conf.overProp(props);
        }
    }

    private IConsumerSender<T> loadSender() {
        IConsumerSender<T> sender = null;

        SenderConsumerEnum senderConsumerEnum = consumer.getSenderConsumerEnum();
        if (senderConsumerEnum == null) {
            log.info("需要配置发送者");
            LoggerUtil.exit(JvmStatus.s15);
        }
        try {
            // 构造参数
            JSONObject senderParams = new JSONObject();
            senderParams.put(ConsumerSenderAbs.RULES_KEY, consumer.getRuleList());

            String pluginJar = senderConsumerEnum.getPluginJar();

            if (StringUtils.isNotEmpty(pluginJar)) {
                // 1.使用的是插件的Sender的情况
                sender = loadPluginSender(senderConsumerEnum, senderParams);
            } else {
                // 2.内置的Sender的情况
                sender = loadBuiltInSender(senderConsumerEnum.getPluginClass(), senderParams);
            }
        } catch (Exception e) {
            log.info("创建发送者时异常", e);
            LoggerUtil.exit(JvmStatus.s15);
        }
        if (sender == null) {
            log.info("需要配置发送者");
            LoggerUtil.exit(JvmStatus.s15);
        }
        return sender;
    }

    private IConsumerSender<T> loadBuiltInSender(String pluginClass, JSONObject senderParams) throws Exception {
        return ConsumerSenderAbs.loadBuiltInSender(pluginClass, senderParams);
    }

    private IConsumerSender<T> loadPluginSender(SenderConsumerEnum pluginSender, JSONObject senderParams) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Plugin plugin = PluginAssit.newPlugin(
                IOUtil.mergeFolderAndFilePath(rootDir.getPath(),
                        pluginSender.getPluginJar()),
                "net.wicp.tams.duckula.plugin.receiver.consumer.IConsumerSender", classLoader,
                "net.wicp.tams.duckula.plugin.receiver.consumer.IConsumerSender",
                "net.wicp.tams.duckula.plugin.receiver.consumer.ConsumerSenderAbs");
        // 替换ClassLoader(双亲委派模型)
        Thread.currentThread().setContextClassLoader(plugin.getLoad().getClassLoader());// 需要加载前设置好classload
        return ConsumerSenderAbs.loadPluginSender(plugin, senderParams);
    }

    @Override
    public Result doWithRecords(List<ConsumerRecord<String, byte[]>> consumerRecords) {
        List<T> datas = new ArrayList<>();
        for (ConsumerRecord<String, byte[]> consumerRecord : consumerRecords) {
            Rule rule = null;
            if (!isIde) {
                DuckulaEvent duckulaEvent = null;
                try {
                    duckulaEvent = DuckulaAssit.parse(consumerRecord.value());
                } catch (InvalidProtocolBufferException e1) {
                    log.error("解析失败", e1);
                    continue;// 不处理此数据
                }
                duckulaEventToDatas(datas, duckulaEvent, rule,null);
            } else {
                DuckulaEventIdempotent duckulaEventIdempotent = null;
                try {
                    duckulaEventIdempotent = DuckulaIdempotentAssit.parse(consumerRecord.value());
                } catch (InvalidProtocolBufferException e1) {
                    log.error("解析失败", e1);
                    continue;// 不处理此数据
                }
                Builder duckulaEventBuilder = DuckulaEvent.newBuilder();
                duckulaEventBuilder.setDb(duckulaEventIdempotent.getDb());
                duckulaEventBuilder.setTb(duckulaEventIdempotent.getTb());
                duckulaEventBuilder.setOptType(duckulaEventIdempotent.getOptType());
                duckulaEventBuilder.addAllCols(duckulaEventIdempotent.getKeyNamesList());
                duckulaEventBuilder.addAllColsType(duckulaEventIdempotent.getKeyTypesList());
                duckulaEventBuilder.setIsError(true);                
                for (IdempotentEle dempotentEle : duckulaEventIdempotent.getValuesList()) {
                    Builder tempEventBuild = duckulaEventBuilder.clone();
                    for (int i = 0; i < dempotentEle.getKeyValuesCount(); i++) {
                        String keyValues = dempotentEle.getKeyValues(i);
                        if (duckulaEventIdempotent.getOptType() == OptType.delete) {
                            tempEventBuild.putBefore(duckulaEventBuilder.getCols(i), keyValues);
                        } else {
                            tempEventBuild.putAfter(duckulaEventBuilder.getCols(i), keyValues);
                        }
                    }
                    duckulaEventToDatas(datas, tempEventBuild.build(), rule,duckulaEventIdempotent.getAddProp());
                }
                
                
            }

        }
        if (datas.size() == 0) {// 没有可用的数据，有可能是合理跳过了某些数据
            return Result.getSuc();
        } else {
            List<T> removeList = new ArrayList<>();
            for (T data : datas) {
                if (sender.checkDataNull(data)) {
                    removeList.add(data);
                }
            }
            if (datas.size() == removeList.size()) {// 没有可用的数据，有可能是合理跳过了某些数据
                return Result.getSuc();
            }
            datas.removeAll(removeList);
        }
        // 插件化
        if (busiEs != null) {
            try {
                this.busiEs.doBusi(datas);
            } catch (Throwable e) {
                log.error("业务处理失败", e);// TODO
                LoggerUtil.exit(JvmStatus.s15);
            }
        }

        // 20190624 增加重试机制
        while (true) {
            try {
                Result sendResult = sender.doSend(datas);
                if (sendResult.isSuc()) {
                    MainConsumer.metric.counter_send_es.inc(datas.size());
                    log.info("this batch sended num：{},all num:{}, max record dept:{},offiset：{}", datas.size(),
                            MainConsumer.metric.counter_send_es.getCount(),
                            consumerRecords.get(consumerRecords.size() - 1).partition(),
                            consumerRecords.get(consumerRecords.size() - 1).offset());
                    TimeAssist.reDoWaitInit("tams-consumer");
                    return Result.getSuc();
                } else {
                    log.error("发送失败，原因:{}", sendResult.getMessage());
                    boolean reDoWait = TimeAssist.reDoWait("tams-consumer", 8);
                    if (reDoWait) {
                        LoggerUtil.exit(JvmStatus.s15);// 达到了最大值，退出
                        return sendResult;
                    }
                }
            } catch (Throwable e) {
                log.error("发送异常", e);
                boolean reDoWait = TimeAssist.reDoWait("tams-consumer", 8);
                if (reDoWait) {
                    LoggerUtil.exit(JvmStatus.s15);// 达到了最大值，退出
                    throw new ProjectExceptionRuntime(ExceptAll.duckula_es_batch);
                }
            }
        }
    }

    /***
     * 数据转换
     *
     * @param datas        要还回的转换数据
     * @param duckulaEvent 原始数据
     * @param rule         规则
     * @param  addProp      附加信息，来自task,用于反查
     */
    private void duckulaEventToDatas(List<T> datas, DuckulaEvent duckulaEvent, Rule rule,String addProp) {
        try {
            if (rule == null) {
                rule = ruleManager.findRule(duckulaEvent.getDb(), duckulaEvent.getTb());
                if (rule == null) {
                    return;
                }
            }
            
           Connection connection = getConn(addProp);
            
            String keymapkey = String.format("%s.%s", duckulaEvent.getDb(), duckulaEvent.getTb());
            if (primarysMap.get(keymapkey) == null) {
                String keyColName = rule.getItems().get(RuleItem.key);
                String[] primarys;
                if (StringUtil.isNull(keyColName)) {
                    primarys = MySqlAssit.getPrimary(connection, duckulaEvent.getDb(), duckulaEvent.getTb());
                } else {
                    primarys = keyColName.split(",");
                }
                primarysMap.put(keymapkey, primarys);
            }
            if (colsMap.get(keymapkey) == null) {
                String[][] cols = MySqlAssit.getCols(connection, duckulaEvent.getDb(), duckulaEvent.getTb(),
                        YesOrNo.no);
                colsMap.put(keymapkey, cols[0]);
            }

            Serializable[] keyValues = new Serializable[primarysMap.get(keymapkey).length];
            for (int i = 0; i < keyValues.length; i++) {
                keyValues[i] = DuckulaAssit.getValue(duckulaEvent, primarysMap.get(keymapkey)[i]);
            }
            String idStr = CollectionUtil.arrayJoin(keyValues, "-");
            Map<String, String> datamap = new HashMap<>();
            if (duckulaEvent.getOptType() == OptType.delete) {
                for (int i = 0; i < keyValues.length; i++) {
                    datamap.put(primarysMap.get(keymapkey)[i], String.valueOf(keyValues[i]));
                }
            } else if (duckulaEvent.getIsError()) {
                StringBuilder build = new StringBuilder();                
               // if(StringUtil.isNotNull(rule.getItems().get(RuleItem.addProp))) {//暂定drds
                String scanstr="";
                build.append("select "+scanstr+" * from " + keymapkey + " where ");
                for (int i = 0; i < primarysMap.get(keymapkey).length; i++) {
                    build.append(String.format(" %s=?", primarysMap.get(keymapkey)[i]));
                }
                PreparedStatement preparedStatement = connection.prepareStatement(build.toString());
                JdbcAssit.setPreParam(preparedStatement, keyValues);
                ResultSet rs = preparedStatement.executeQuery();
                if (isIde||YesOrNo.yes==this.consumer.getNeedAllCol()) {
                    if (rs.next()) {
                        for (String colName : colsMap.get(keymapkey)) {
                            try {
                                String valuestr = rs.getString(colName);
                                datamap.put(colName, valuestr);
                            } catch (Exception e) {
                                log.error("no colName:" + colName, e);
                            }
                        }
                    } else {
                        log.error("没有找到ID:{}", idStr);
                        return;
                    }
                    // List<Map<String, String>> rsToMap = JdbcAssit.rsToMap(rs);
                    // rs.last(); // 20191202 Operation not allowed after ResultSet closed
                    // https://www.cnblogs.com/haore147/p/3617767.html
                    // if (CollectionUtils.isNotEmpty(rsToMap)) {
                    // datamap.putAll(rsToMap.get(0));
                    // } else {
                    // log.error("没有找到ID:{}", idStr);
                    // return;
                    // }
                } else {
                    if (rs.next()) {
                        for (String colName : duckulaEvent.getColsList()) {
                            String valuestr = rs.getString(colName);
                            datamap.put(colName, valuestr);
                        }
                    } else {
                        log.error("没有找到ID:{}", idStr);
                        return;
                    }
                }
                try {
                    rs.close();
                    preparedStatement.close();
                    connection.close();
                } catch (Exception e) {
                    log.error("关闭es失败", e);
                }
            } else {
                for (String colName : duckulaEvent.getColsList()) {
                    String valuestr = DuckulaAssit.getValueStr(duckulaEvent, colName);
                    datamap.put(colName, valuestr);
                }
            }
            CollectionUtil.filterNull(datamap, 1);
            T packObj = sender.packObj(duckulaEvent, datamap, rule, this.primarysMap.get(keymapkey));
            if (packObj != null) {// 没有错误，有些错误需要跳过
                datas.add(packObj);
            }
        } catch (Exception e) {
            log.error("组装失败", e);
            LoggerUtil.exit(JvmStatus.s15);
            throw new ProjectExceptionRuntime(ExceptAll.duckula_es_formate);
        }
    }

	private Connection getConn(String addProp) throws SQLException {
		Connection connection = null;
		if(StringUtil.isNotNull(addProp)) {
			//通过SQL路由失败
			//DbInstance dbInstance = JSONObject.toJavaObject(ZkClient.getInst().getZkData(ZkPath.dbinsts.getPath(consumer.getDrdsDbInst())),
			//		DbInstance.class);               	
			//int lastIndexOf = duckulaEvent.getDb().lastIndexOf("_");
			//int scandb = Integer.parseInt(duckulaEvent.getDb().substring(lastIndexOf+1));
			//scanstr= "/*+TDDL:scan(node='"+scandb+"')*/";
			//Properties props=new Properties();
			//props.put("host", dbInstance.getUrl());
			//props.put("username", dbInstance.getUser());
			//props.put("password", dbInstance.getPwd());
			//props.put("port", dbInstance.getPort());               	
			//connection = DruidAssit.getDataSourceNoConf("drds", props).getConnection();
			String sourceKey="drds:"+addProp;
			if(!DruidAssit.getDataSourceMap().containsKey(sourceKey)) {
				DbInstance dbInstance = JSONObject.toJavaObject(ZkClient.getInst().getZkData(ZkPath.dbinsts.getPath(addProp)),
						DbInstance.class);
		    	Properties props=new Properties();
		    	props.put("host", dbInstance.getUrl());
		    	props.put("username", dbInstance.getUser());
		    	props.put("password", dbInstance.getPwd());
		    	props.put("port", dbInstance.getPort());               	
		    	connection = DruidAssit.getDataSourceNoConf(sourceKey, props).getConnection();   
			}else {
				connection = DruidAssit.getDataSource(sourceKey).getConnection();
			}
			
		}else {
			connection = DruidAssit.getConnection();
		}
		return connection;
	}

    /**
     * 获取消息数据保存新的文件
     *
     * @param bucketName
     * @param keyName
     * @param savePath
     */
    private void getObjectForFile(AmazonS3 s3, String bucketName, String keyName, String savePath) {
        S3ObjectInputStream s3is = null;
        FileOutputStream fos = null;
        try {
            S3Object o = s3.getObject(bucketName, keyName);
            s3is = o.getObjectContent();
            fos = new FileOutputStream(new File(StringUtil.isNotNull(savePath) ? savePath : keyName));
            IOUtil.copyInToOut(s3is, fos);
        } catch (Exception e) {
            log.error("取得s3文件失败", e);
            throw new RuntimeException("取得s3文件失败");
        } finally {
            try {
                if (s3is != null) {
                    s3is.close();
                }
            } catch (IOException e) {
            }
            try {
                if (fos != null) {
                    fos.close();
                }
            } catch (Exception e2) {
            }
        }
    }
}
