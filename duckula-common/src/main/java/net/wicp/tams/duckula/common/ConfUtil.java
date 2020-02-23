package net.wicp.tams.duckula.common;

import java.io.File;
import java.util.Properties;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.duckula.common.constant.MiddlewareType;

public abstract class ConfUtil {
	public static int defaulJmxPort=2723;
	public static int defaultDebugPort=2113;
	
	//public static String drdsPattern="0-9a-zA-Z";
	
	public static String drdsTbPatternFormat1="^%s_[0-9a-zA-Z]{4}";
	
	public static String drdsTbPatternFormat2="^%s_[0-9a-zA-Z]{4}_[0-9]{2,}$";
	
	public static String[]  skipdbs=new String[] {"mysql"};//需要跳过的库名
	
	public static String[]  skiptbs=new String[] {"__drds__systable__leadership__"};//需要跳过的表名
	
	public static Properties configMiddleware(MiddlewareType middlewareType, String middlewareInst) {
		String mergeFolderAndFilePath = IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_DATA"),
				String.format("/conf/%s/%s-%s.properties", middlewareType, middlewareType, middlewareInst));
		Properties retProps = IOUtil.fileToProperties(new File(mergeFolderAndFilePath));
		return retProps;
	}
	
	public static String getDatadir(boolean isconfig) {
		String datadir =isconfig? Conf.get("duckula.ops.datadir"):System.getenv("DUCKULA_DATA");
		return datadir;
	}
	
	public static JSONObject getConfigGlobal() {
		JSONObject zkData = ZkClient.getInst().getZkData(Conf.get("duckula.zk.rootpath"));
		JSONArray arrays = zkData.getJSONArray("rows");
		JSONObject retobj = new JSONObject();
		for (Object object : arrays) {
			JSONObject data = (JSONObject) object;
			retobj.put(data.getString("name"), data.getString("value"));
		}
		return retobj;
	};
	
	public static void printlnASCII() {
		String formatestr="-----------------------------------  %s  --------------------------------------------";		
		System.out.println(String.format(formatestr, "     _            _          _       ___   "));
		System.out.println(String.format(formatestr, "    | |          | |        | |     |__ \\  "));
		System.out.println(String.format(formatestr, "  __| |_   _  ___| | ___   _| | __ _   ) | "));
		System.out.println(String.format(formatestr, " / _` | | | |/ __| |/ / | | | |/ _`  | / /  "));
		System.out.println(String.format(formatestr, "| (_| | |_| | (__|   <| |_| | | (_| |/ /_  "));
		System.out.println(String.format(formatestr, " \\__,_|\\__,_|\\___|_|\\_\\\\__,_|_|\\__,_|____| "));
		System.out.println(String.format(formatestr, "                                           "));
	}
}
