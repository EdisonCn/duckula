package net.wicp.tams.duckula.common.beans;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.duckula.common.constant.BusiEnum;
import net.wicp.tams.duckula.common.constant.MiddlewareType;
import net.wicp.tams.duckula.common.constant.SenderEnum;
import net.wicp.tams.duckula.common.constant.SerializerEnum;
import net.wicp.tams.duckula.plugin.beans.Rule;

/***
 * 表
 * 
 * @author zhoujunhui
 *
 */
@Data
@Slf4j
public class Task {
	private String id;// 唯一标识
	private String ip;// ip地址或机器名
	private int clientId;// 连到mysql的从服务器ID
	private int port;
	private String dbinst;// 数据库实例名//只用于位点历史更新的挂存目录用，不做其它使用
	private YesOrNo rds = YesOrNo.yes;// yes表示是rds
	private YesOrNo isSsh = YesOrNo.no; // no表示不启用ssh
	private YesOrNo posListener = YesOrNo.yes; // yes表示需要启用位点上传
	private String user;
	private String pwd;
	private String defaultDb;
	private String imageVersion = Conf.get("duckula.task.image.tag"); // task的image版本
	private String namespace = Conf.get("common.kubernetes.apiserver.namespace.default"); // k8s的命名空间
	
	//private String colNames;//要附加的字段名
	//private String addPropsJsonStr;//要附加的属性值
	

	

	public String getImageVersion() {
		return StringUtil.isNull(this.imageVersion) ? Conf.get("duckula.task.image.tag") : this.imageVersion;
	}

	public String getNamespace() {
		return StringUtil.isNull(this.namespace) ? Conf.get("common.kubernetes.apiserver.namespace.default")
				: this.namespace;
	}

	private String rules;// 规则：demo`user`{'key':'aaa:%s'} eg:
							// demo,policy_0000,id,demo_policy|demo,sdk_info_0000,id,demo_policy
	private String beginTime;// 任务支持binlog的时间,默认为创建任务时的时间
	private YesOrNo run = YesOrNo.no;// 是否运行此任务,默认为false不运行,仅配置好,不做运行处理.

	private final List<Rule> ruleList = new ArrayList<>();

	// private String dbPattern;// db的模式
	// private String tbPattern;// table的模式
	// private String splitKey;//分库分表键

	private String receivePluginDir;
	private SenderEnum senderEnum;
	private SerializerEnum serializerEnum;
	private BusiEnum busiEnum;
	private String busiPluginDir;
	private Map<String, String> params;
	private String remark;
	private int threadNum = 1;// 线程数，对于kafka将不起作用
	private int queueSize;// 循环队列大小

	private MiddlewareType middlewareType;// 中间件类型

	private String middlewareInst;// 中间件配置

	public String getReceivePluginDir() {
		if (senderEnum == SenderEnum.no) {
			return this.receivePluginDir;
		} else {
			return senderEnum.getPluginJar();
		}
	}

	public String getBusiDowithPluginDir() {
		if (busiEnum == null) {
			return "";
		} else if (busiEnum == BusiEnum.custom) {
			return this.busiPluginDir;
		} else {
			return busiEnum.getPluginJar();
		}
	}

	public void setRules(String rules) {
		this.rules = rules;
		this.ruleList.clear();
		List<Rule> buildRules = Rule.buildRules(rules);
		this.ruleList.addAll(buildRules);
	}
}
