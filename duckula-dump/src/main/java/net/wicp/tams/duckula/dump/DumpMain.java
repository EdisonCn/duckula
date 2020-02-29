package net.wicp.tams.duckula.dump;

import java.io.IOException;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.lang3.ArrayUtils;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.OSinfo;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.binlog.dump.MainDump;
import net.wicp.tams.common.constant.DateFormatCase;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.common.metrics.utility.TsLogger;
import net.wicp.tams.duckula.common.ConfUtil;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.Dump;
import net.wicp.tams.duckula.common.beans.Task;
import net.wicp.tams.duckula.common.constant.CommandType;
import net.wicp.tams.duckula.common.constant.MiddlewareType;
import net.wicp.tams.duckula.common.constant.ZkPath;

@Slf4j
public class DumpMain {
	public static void main(String[] args)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException {
		Thread.currentThread().setName("Dump-main");
		if (ArrayUtils.isEmpty(args)) {
			System.err.println("----未传入taskid，不能启动task----");
			log.error("----未传入taskid，不能启动task----");
			return;
		}
		String dumpId = args[0];
		log.info("----------------------加载配置文件-------------------------------------");
		CommandType.dump.setCommonProps();
		// 加载目的地中间件
		Dump dump = ZkUtil.buidlDump(dumpId);
		Properties configMiddleware = ConfUtil.configMiddleware(MiddlewareType.es, dump.getCluster());// TODO
																										// 满足不同的存储中间件，并不只是ES
		Conf.overProp(configMiddleware);

		log.info("----------------------创建zk临时文件-------------------------------------");
		String curtimestr = DateFormatCase.yyyyMMddHHmmss.getInstanc().format(new Date());
		String tempNodePath = IOUtil.mergeFolderAndFilePath(ZkPath.dumps.getPath(dumpId), curtimestr);
		String ip = "unknow";
		try {
			InetAddress addr = OSinfo.findFirstNonLoopbackAddress();
			ip = addr.getHostAddress().toString(); // 获取本机ip
		} catch (Exception e) {
			log.error("获取本机IP失败");
		}
		ZkClient.getInst().createNode(tempNodePath, ip, true);
		log.info("----------------------配置metrix-------------------------------------");
		System.setProperty(TsLogger.ENV_FILE_NAME, "dump_" + dumpId);
		System.setProperty(TsLogger.ENV_FILE_ROOT, String.format("%s/logs/metrics", System.getenv("DUCKULA_DATA")));
		log.info("----------------------导入配置-------------------------------------");
		Task task = ZkUtil.buidlTask(dump.getTaskOnlineId());
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
		Conf.overProp(props);

		MainDump main = new MainDump();
		main.dump();
		System.in.read();
	}
}
