package net.wicp.tams.duckula.dump;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.ArrayUtils;

import com.alibaba.fastjson.JSONObject;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.OSinfo;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.jdbc.MySqlAssit;
import net.wicp.tams.common.binlog.dump.MainDump;
import net.wicp.tams.common.constant.DateFormatCase;
import net.wicp.tams.common.jdbc.DruidAssit;
import net.wicp.tams.common.metrics.utility.TsLogger;
import net.wicp.tams.duckula.common.ConfUtil;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.DbInstance;
import net.wicp.tams.duckula.common.beans.Dump;
import net.wicp.tams.duckula.common.constant.CommandType;
import net.wicp.tams.duckula.common.constant.DumpEnum;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.plugin.beans.Rule;

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
		if (dump.getMiddlewareType() != null) {
			Properties configMiddleware = ConfUtil.configMiddleware(dump.getMiddlewareType(), dump.getMiddlewareInst());
			Conf.overProp(configMiddleware);
		}

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
		Properties props = new Properties();
		props.put("common.binlog.alone.dump.global.enable", "true");//
		DbInstance dbInstance = ZkClient.getInst().getDateObj(ZkPath.dbinsts.getPath(dump.getDbinst()),
				DbInstance.class);
		props.put("common.binlog.alone.dump.global.pool.host", dbInstance.getUrl());
		props.put("common.binlog.alone.dump.global.pool.port", String.valueOf(dbInstance.getPort()));
		props.put("common.binlog.alone.dump.global.pool.username", dbInstance.getUser());
		props.put("common.binlog.alone.dump.global.pool.password", dbInstance.getPwd());
		DumpEnum dumpEnum = dump.getDumpEnum();
		if (dumpEnum != null && StringUtil.isNotNull(dumpEnum.getPluginJar())) {// 插件处理
			File rootDir = new File(System.getenv("DUCKULA_DATA"));
			String pluginDri = IOUtil.mergeFolderAndFilePath(rootDir.getPath(), dumpEnum.getPluginJar());
			props.put("common.binlog.alone.dump.global.busiPluginDir", pluginDri);
		}
		Conf.overProp(props);
		log.info("----------------------处理原文件配置-------------------------------------");
		Properties newprops = Conf.replacePre("common.binlog.alone.dump.global.pool",
				"common.jdbc.datasource." + MainDump.globleDatasourceName);
		Conf.overProp(newprops);
		Connection conn = DruidAssit.getConnection(MainDump.globleDatasourceName);
		Properties dumpProps = new Properties();
		List<String> dumpIds = new ArrayList<String>();
		for (Rule rule : dump.getRuleList()) {
			List<String[]> allTables = MySqlAssit.getAllTables(conn, rule.getDbPattern(), rule.getTbPattern());
			// rule.getItems().get(RuleItem.index);
			for (String[] dbtb : allTables) {
				String dumpIdTemp = dbtb[0] + "-" + dbtb[1];
				dumpProps.put(String.format("common.binlog.alone.dump.ori.%s.db", dumpIdTemp), dbtb[0]);
				dumpProps.put(String.format("common.binlog.alone.dump.ori.%s.tb", dumpIdTemp), dbtb[1]);
				dumpIds.add(dumpIdTemp);
			}
		}
		log.info("----------------------插件处理配置-------------------------------------");
		if (StringUtil.isNotNull(dump.getDumpEnum().getPluginJar())) {// 有插件
			String pluginDir = IOUtil.mergeFolderAndFilePath(ConfUtil.getDatadir(false),
					dump.getDumpEnum().getPluginJar());
			dumpProps.put("common.binlog.alone.dump.global.busiPluginDir", pluginDir);
		}

		if (StringUtil.isNotNull(dump.getDumpEnum().getPluginClassName())) {// mysql是内置的
			dumpProps.put("common.binlog.alone.dump.global.ori.busiSender", dump.getDumpEnum().getPluginClassName());
		}
		Conf.overProp(dumpProps);
		JSONObject params = new JSONObject();
		params.put("middlewareType", dump.getMiddlewareType() != null ? dump.getMiddlewareType().name() : "");
		params.put("middlewareInst", dump.getMiddlewareType() != null ? dump.getMiddlewareInst() : "");
		params.put("rules", dump.getRules());
		MainDump main = new MainDump();
		main.dump(params);
		System.in.read();
	}
}