package net.wicp.tams.duckula.common.beans;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.duckula.common.constant.MiddlewareType;
import net.wicp.tams.duckula.plugin.beans.Rule;

@Data
@Slf4j
public class Consumer {
	private String id;
	private String taskOnlineId;
	private String topic;
	private int partitionNum;// 分区数，在ops需要查到
	private String groupId;// 使用哪个groupId，为空则用默认的groupId
	private Long startPosition;// 从哪个位置开始监听
	private YesOrNo needAllCol = YesOrNo.no;//是否需要全量字段处理，出现在做了列过滤后，又想要在ES做全字段索引等
	private String rules;
	private YesOrNo run = YesOrNo.no;// 是否运行此任务,默认为false不运行,仅配置好,不做运行处理.
	
	private String drdsDbInst;// 数据库实例名,用于drds反查配置
	
	//
//    private SenderConsumerEnum senderConsumerEnum; // 内置发送者
//    private ConsumerSenderPluginEnum pluginSender; // 插件发送者
	// 内置发送者和插件发送者合并
	private SenderConsumerEnum senderConsumerEnum;

	private final List<Rule> ruleList = new ArrayList<>();

	private MiddlewareType middlewareType;// 中间件类型
	private String middlewareInst;// 中间件配置

	// k8s版本使用的CPU和内存
	private Integer cpu;// CPU数据
	private Integer memory;// 内存 M

	//
	private Integer busiNum;
	private String busiPlugin;

	private Integer batchNum = 50;// 每次拉取数量
	private Integer batchTimeout = 1000;// 拉取的超时时间

	public void setRules(String rules) {
		this.rules = rules;
		this.ruleList.clear();
		List<Rule> buildRules = Rule.buildRules(rules);
		this.ruleList.addAll(buildRules);
	}

}
