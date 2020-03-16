package net.wicp.tams.duckula.common.beans;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.duckula.common.constant.DumpEnum;
import net.wicp.tams.duckula.common.constant.MiddlewareType;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.constant.RuleItem;

@Data
@Slf4j
public class Dump {
	private String id;
	private String dbinst;// 数据库实例Id
	// private String cluster;// es的集群配置，就是conf下面的配置
	// private String mappingId;
	// private String db_tb;
	// private String[] primarys;
	private Integer numDuan;
	private String schedule;// 如果是定时器模式才设置
	// private String wheresql;// where语句
	private String remark;

	private String busiPlugin;// 业务用的插件
	// 多线程配置
	private Integer baseDataNum;// 抽数据线程数
	private Integer busiNum;// 业务处理线程数
	private Integer sendNum;// 发送线程数
	// k8s版本使用的CPU和内存
	private Integer cpu;// CPU数据
	private Integer memory;// 内存 M

	private DumpEnum dumpEnum;
	private MiddlewareType middlewareType;// 中间件类型
	private String middlewareInst;// 中间件配置
	private String rules;

	private String imageVersion;
	private String namespace;

	private final List<Rule> ruleList = new ArrayList<>();

	public void setRules(String rules) {
		this.rules = rules;
		this.ruleList.clear();
		List<Rule> buildRules = Rule.buildRules(rules);
		this.ruleList.addAll(buildRules);
	}

	/*
	 * public String packFromstr() { if (StringUtil.isNotNull(this.wheresql)) {
	 * this.wheresql = StringUtil.trimSpace(this.wheresql); if
	 * (!this.getWheresql().substring(0, 5).equalsIgnoreCase("where")) {
	 * this.wheresql = "where " + this.wheresql; } } String fromstr =
	 * String.format("from %s %s", this.getDb_tb(),
	 * StringUtil.isNull(this.getWheresql()) ? "where 1=1 " : this.getWheresql());
	 * return fromstr; }
	 */
}
