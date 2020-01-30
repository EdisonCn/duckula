package net.wicp.tams.duckula.common.beans;

import lombok.Data;
import net.wicp.tams.duckula.common.constant.PluginType;

@Data
public class BusiPlugin {
	private String id;
	private String projectName;// 项目名
	private PluginType pluginType;//插件类型 
	private String update;// 上传人
	private String lastUpdateTime;// yyyy-MM-dd hh:mm:ss
	private String pluginFileName;
}
