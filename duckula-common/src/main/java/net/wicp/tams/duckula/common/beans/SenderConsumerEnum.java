package net.wicp.tams.duckula.common.beans;

import net.wicp.tams.common.constant.dic.intf.IEnumCombobox;
import net.wicp.tams.duckula.common.constant.SenderEnum;

/**
 * consumer的发送者
 *
 * @author 偏锋书生
 * <p>
 * 2018年6月28日
 */
public enum SenderConsumerEnum implements IEnumCombobox {

//    es("es搜索", "net.wicp.tams.duckula.kafka.consumer.impl.ConsumerSenderEsImpl", ""),

    es6("es6", "", "/sender/duckula-plugin-es6"),

    es7("es7", "", "/sender/duckula-plugin-es7/"),

    jdbc("mysql", "net.wicp.tams.duckula.kafka.consumer.impl.ConsumerSenderMysqlImpl", ""), // TODO 可以选择配置到哪个数据库

    kafka("kafka", "net.wicp.tams.duckula.kafka.consumer.impl.ConsumerSenderKafkaImpl", ""),

    no("no", "net.wicp.tams.duckula.kafka.consumer.impl.ConsumerSenderNothingImpl", "");

    private final String desc;
    private final String pluginClass;// 插件类
    private final String pluginJar;// 插件包

    public String getPluginClass() {
        return pluginClass;
    }

    public String getPluginJar() {
        return pluginJar;
    }

    private SenderConsumerEnum(String desc, String pluginClass, String pluginJar) {
        this.desc = desc;
        this.pluginClass = pluginClass;
        this.pluginJar = pluginJar;
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
