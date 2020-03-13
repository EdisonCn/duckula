package net.wicp.tams.duckula.ops.beans;

import lombok.Data;
import net.wicp.tams.duckula.common.constant.MiddlewareType;

@Data
public class StartK8sBean {
	private String imageVersion;
	private String namespace;
	private MiddlewareType middlewareType;// 中间件类型
	private String middlewareInst;// 中间件配置
}
