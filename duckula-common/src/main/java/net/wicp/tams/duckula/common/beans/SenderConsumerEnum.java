package net.wicp.tams.duckula.common.beans;

import net.wicp.tams.common.constant.dic.intf.IEnumCombobox;
import net.wicp.tams.duckula.common.constant.SenderEnum;

/**
 * consumer的发送者
 * 
 * @author 偏锋书生
 *
 *         2018年6月28日
 */
public enum SenderConsumerEnum implements IEnumCombobox {

	es("es搜索", "net.wicp.tams.duckula.kafka.consumer.impl.ConsumerEsImpl"),

	jdbc("mysql", "net.wicp.tams.duckula.kafka.consumer.impl.ConsumerMysqlImpl"), // TODO 可以选择配置到哪个数据库

	kafka("kafka", "net.wicp.tams.duckula.kafka.consumer.impl.ConsumerKafkaImpl"),

	no("no", "net.wicp.tams.duckula.kafka.consumer.impl.ConsumerNothingImpl");

	private final String desc;
	private final String pluginClass;// 值

	public String getPluginClass() {
		return pluginClass;
	}

	private SenderConsumerEnum(String desc, String pluginClass) {
		this.desc = desc;
		this.pluginClass = pluginClass;
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

}
