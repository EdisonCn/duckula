package net.wicp.tams.duckula.common.constant;

import net.wicp.tams.common.constant.dic.intf.IEnumCombobox;

public enum DumpEnum implements IEnumCombobox {

	es_v6("es搜索(v<7)", "/dumpsender/duckula-dump-es6/", "net.wicp.tams.duckula.dump.es6.SenderEs", MiddlewareType.es),

	es_v7("es搜索(v=7.x)", "/dumpsender/duckula-dump-es7/", "net.wicp.tams.duckula.dump.es7.SenderEs", MiddlewareType.es),

	mysql("mysql发送者", "", "mysql", null),

	no("其它发送者", "", "", null);

	private final String desc;
	private final String pluginJar;// 值
	private final String pluginClassName;// 值

	private final MiddlewareType middlewareType;// 关联的中间件类型

	public String getPluginJar() {
		// String pathStr = IOUtil.mergeFolderAndFilePath(rootDir.getPath(),
		// this.pluginJar);
		return pluginJar;
	}

	private DumpEnum(String desc, String pluginJar, String pluginClassName, MiddlewareType middlewareType) {
		this.desc = desc;
		this.pluginJar = pluginJar;
		this.pluginClassName = pluginClassName;
		this.middlewareType = middlewareType;
	}

	public static DumpEnum get(String name) {
		for (DumpEnum senderEnum : DumpEnum.values()) {
			if (senderEnum.name().equalsIgnoreCase(name)) {
				return senderEnum;
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

	public MiddlewareType getMiddlewareType() {
		return middlewareType;
	}

	public String getPluginClassName() {
		return pluginClassName;
	}
}
