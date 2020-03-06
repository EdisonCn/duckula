package net.wicp.tams.duckula.common.constant;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Plugin;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.constant.dic.intf.IEnumCombobox;
import net.wicp.tams.duckula.common.ConfUtil;
import net.wicp.tams.duckula.plugin.IOps;
import net.wicp.tams.duckula.plugin.PluginAssit;

import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;

/**
 * ops用的插件管理
 *
 * @author 偏锋书生
 * <p>
 * 2018年6月28日
 */
@Slf4j
public enum OpsPlugEnum implements IEnumCombobox {

    es6("es6", "net.wicp.tams.duckula.plugin.es6.Es6Ops", "/sender/duckula-plugin-es6/"),

    es7("es7", "net.wicp.tams.duckula.plugin.es7.Es7Ops", "/sender/duckula-plugin-es7/");

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
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        String plugDirSys = IOUtil.mergeFolderAndFilePath(ConfUtil.getDatadir(false), pluginJar);
        Plugin plugin = PluginAssit.newPlugin(plugDirSys,
                pluginClass, classLoader,
                "net.wicp.tams.duckula.plugin.IOps");
        this.classLoader = plugin.getLoad().getClassLoader();
    }

    public IOps newOps() {
        try {
            // 切换ClassLoader再拿Ops插件实例
            Thread.currentThread().setContextClassLoader(this.classLoader);// 需要加载前设置好classload
            return (IOps) this.classLoader.loadClass(this.pluginClass).newInstance();
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
