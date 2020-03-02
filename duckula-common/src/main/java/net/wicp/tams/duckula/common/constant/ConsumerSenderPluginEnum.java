//package net.wicp.tams.duckula.common.constant;
//
//import net.wicp.tams.common.constant.dic.intf.IEnumCombobox;
//
//public enum ConsumerSenderPluginEnum implements IEnumCombobox {
//    es6("es6", "/consumersender/duckula-consumersender-es6/"),
//
//    es7("es7", "/consumersender/duckula-consumersender-es7/");
//
//    private final String desc;
//    private final String pluginJar;// 值
//
//    public String getPluginJar() {
//        // String pathStr =
//        // IOUtil.mergeFolderAndFilePath(SenderEnum.rootDir.getPath(),
//        // this.pluginJar);
//        return pluginJar;
//    }
//
//    private ConsumerSenderPluginEnum(String desc, String pluginJar) {
//        this.desc = desc;
//        this.pluginJar = pluginJar;
//    }
//
//    public static ConsumerSenderPluginEnum get(String name) {
//        for (ConsumerSenderPluginEnum serialEnum : ConsumerSenderPluginEnum.values()) {
//            if (serialEnum.name().equalsIgnoreCase(name)) {
//                return serialEnum;
//            }
//        }
//        return null;
//    }
//
//    public String getDesc() {
//        return desc;
//    }
//
//    @Override
//    public String getName() {
//        return this.name();
//    }
//
//    @Override
//    public String getDesc_en() {
//        return this.desc;
//    }
//
//    @Override
//    public String getDesc_zh() {
//        return this.desc;
//    }
//}
