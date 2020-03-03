package net.wicp.tams.duckula.common.constant;

import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.ClassLoaderPlugin;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.constant.dic.intf.IEnumCombobox;
import net.wicp.tams.duckula.common.ConfUtil;
import net.wicp.tams.duckula.common.constant.SenderEnum;
import net.wicp.tams.duckula.plugin.IOps;

/**
 * ops用的插件管理
 *
 * @author 偏锋书生
 *         <p>
 *         2018年6月28日
 */
@Slf4j
public enum OpsPlugEnum implements IEnumCombobox {

	es6("es6", "net.wicp.tams.duckula.plugin.es6.OpsPluginImpl", "/sender/duckula-plugin-elasticsearch/"),

	es7("es7", "", "/sender/duckula-plugin-es7/");

	private final String desc;
	private final String pluginClass;// 插件类
	private final String pluginJar;// 插件包
	private final URLClassLoader classLoader;

	public String getPluginClass() {
		return pluginClass;
	}

	public String getPluginJar() {
		return pluginJar;
	}

	private OpsPlugEnum(String desc, String pluginClass, String pluginJar) {
		this.desc = desc;
		this.pluginClass = pluginClass;
		this.pluginJar = pluginJar;
		this.classLoader = new ClassLoaderPlugin(
				IOUtil.mergeFolderAndFilePath(ConfUtil.getDatadir(false), this.pluginJar),
				Thread.currentThread().getContextClassLoader(), 1).getClassLoader();
		Thread.currentThread().setContextClassLoader(this.classLoader);// 需要加载前设置好classload
	}

	public IOps newOps() {
		try {
			Object newInstance = this.classLoader.loadClass(this.pluginClass).newInstance();
			return (IOps) newInstance;
		} catch (Exception e) {
			log.error("load the class error:" + this.pluginClass, e);
			return null;
		}
	}

	public static OpsPlugEnum get(String name) {
		for (OpsPlugEnum opsPlugEnum : OpsPlugEnum.values()) {
			if (opsPlugEnum.name().equalsIgnoreCase(name)) {
				return opsPlugEnum;
			}
		}
		return null;
	}

	public String getDesc() {
		return desc;
	}

	@Override
	public String getName() {
		return this.name();
	}

	@Override
	public String getDesc_en() {
		return this.desc;
	}

	@Override
	public String getDesc_zh() {
		return this.desc;
	}

	public static Map<OpsPlugEnum, URLClassLoader> classloaderMap = new HashMap<OpsPlugEnum, URLClassLoader>();

}
