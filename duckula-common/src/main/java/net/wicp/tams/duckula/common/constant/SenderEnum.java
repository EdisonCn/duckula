package net.wicp.tams.duckula.common.constant;

import net.wicp.tams.common.constant.dic.intf.IEnumCombobox;

public enum SenderEnum implements IEnumCombobox {
	// log("日志服务", "net.wicp.tams.commons.binlog.sender.impl.LogFile"),

	kafkaIde("kafka幂等消息", "/sender/duckula-plugin-kafka-idempotent/", MiddlewareType.kafka, true, null),

	kafka("kafka消息", "/sender/duckula-plugin-kafka/", MiddlewareType.kafka, false, null),

	es("es搜索(v<7)", "/sender/duckula-plugin-es6/", MiddlewareType.es, false, OpsPlugEnum.es6),

	es_v7("es搜索(v=7.x)", "/sender/duckula-plugin-es7/", MiddlewareType.es, false, OpsPlugEnum.es7),

	redis("redis缓存", "/sender/duckula-plugin-redis/", MiddlewareType.redis, false, null),

	cassandra("cassandra存储", "/sender/duckula-plugin-cassandra/", MiddlewareType.redis, false, null),

	// ons("ons消息", "net.wicp.tams.commons.binlog.sender.impl.SenderOns"),

	no("其它发送者", "", null, false, null);

	private final String desc;
	private final String pluginJar;// 值
	private final MiddlewareType middlewareType;// 关联的中间件类型
	private final boolean idempotent;// 是全幂等模式
	private final OpsPlugEnum opsPlugEnum;

	public OpsPlugEnum getOpsPlugEnum() {
		return opsPlugEnum;
	}

	public boolean isIdempotent() {
		return idempotent;
	}

	public String getPluginJar() {
		// String pathStr = IOUtil.mergeFolderAndFilePath(rootDir.getPath(),
		// this.pluginJar);
		return pluginJar;
	}

	private SenderEnum(String desc, String pluginJar, MiddlewareType middlewareType, boolean idempotent,
			OpsPlugEnum opsPlugEnum) {
		this.desc = desc;
		this.pluginJar = pluginJar;
		this.middlewareType = middlewareType;
		this.idempotent = idempotent;
		this.opsPlugEnum = opsPlugEnum;
	}

	public static SenderEnum get(String name) {
		for (SenderEnum senderEnum : SenderEnum.values()) {
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
}
