package net.wicp.tams.duckula.plugin.es6;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.binlog.dump.bean.DumpEvent;
import net.wicp.tams.common.binlog.dump.listener.IBusiSender;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.common.constant.StrPattern;
import net.wicp.tams.common.es.*;
import net.wicp.tams.common.es.EsData.Builder;
import net.wicp.tams.common.es.bean.MappingBean;
import net.wicp.tams.common.es.bean.MappingBean.Propertie;
import net.wicp.tams.common.es.client.ESClient;
import net.wicp.tams.common.es.client.singleton.ESClientOnlyOne;
import net.wicp.tams.common.es.client.threadlocal.EsClientThreadlocal;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.constant.RuleItem;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/****
 * 需要创建好索引eg: IndexParamsBean indexParamsBean =
 * IndexParamsBean.builder().db("test").tb("user_info").db1("test")
 * .tb1("user_addr").rela1("user_id").build();
 * System.out.println(indexParamsBean); Result result =
 * ESClientOnlyOne.getInst().getESClient().indexCreate(conn, indexParamsBean,
 * "test2", 2, 0);
 * 
 * @author andy.zhou
 *
 */
@Slf4j
public class Es6Dumper implements IBusiSender<DumpEvent> {
	private static final org.slf4j.Logger errorlog = org.slf4j.LoggerFactory.getLogger("errorBinlog");
	// private JSONObject relaObj;// relaObj.isEmpty 是已初始化但没有关联关系的索引
	private volatile boolean isInit = false;
	private final Map<String, Rule> ruleMap = new HashMap<String, Rule>();
	private Map<Rule, JSONObject> relaObjMap = new HashMap<Rule, JSONObject>();
	private List<Rule> rulesList;

	/**
	 * 需要的参数：rules、middlewareType、middlewareInst
	 */
	@Override
	public synchronized void initParams(JSONObject params) {
		if (isInit) {
			return;
		}
		String rules = params.getString("rules");
		rulesList = Rule.buildRules(rules);
		Properties configMiddleware = configMiddleware(params.getString("middlewareType"),
				params.getString("middlewareInst"));
		Conf.overProp(configMiddleware);

		for (Rule rule : rulesList) {
			JSONObject relaObj = null;
			Map<String, Propertie> queryMapping_tc_all = ESClientOnlyOne.getInst().getESClient().queryMapping_tc_all(
					rule.getItems().get(RuleItem.index),
					StringUtil.hasNull(rule.getItems().get(RuleItem.type), "_doc"));
			if (queryMapping_tc_all.containsKey(Conf.get("common.es.assit.rela.key"))) {//含有join
				relaObj = queryMapping_tc_all.get(Conf.get("common.es.assit.rela.key")).getRelations();
			} else {
				relaObj = new JSONObject();
			}
			relaObjMap.put(rule, relaObj);
		}
		isInit = true;
	}

	// dumpId是指配置文件中的dump
	@Override
	public void init(String dumpId) {
		return;
	}

	@Override
	public void doSend(DumpEvent dataBuilders) {
		String key = String.format("%s.%s", dataBuilders.getDump().getDb(), dataBuilders.getDump().getTb());
		if (!ruleMap.containsKey(key)) {
			for (Rule rule : rulesList) {
				if (StrPattern.checkStrFormat(rule.getDbPattern(), dataBuilders.getDump().getDb())
						&& StrPattern.checkStrFormat(rule.getTbPattern(), dataBuilders.getDump().getTb())) {
					ruleMap.put(key, rule);
					break;
				}
			}
		}
		Rule curRule = ruleMap.get(key);
		String index = curRule.getItems().get(RuleItem.index);
		ESClient eSClient = EsClientThreadlocal.createPerThreadEsClient();
		Builder esDataBuilder = EsData.newBuilder();
		esDataBuilder.setIndex(index);
		esDataBuilder.setType(StringUtil.hasNull(curRule.getItems().get(RuleItem.type), "_doc"));
		esDataBuilder.setAction(Action.update);
		esDataBuilder.setUpdateSet(UpdateSet.newBuilder().setUpsert(true).build());
		String[] primarys = dataBuilders.getDump().getPrimarys();
		boolean hasprimarys = ArrayUtils.isNotEmpty(primarys);
		for (Map<String, String> datamap : dataBuilders.getDatas()) {
			EsObj.Builder esObjBuilder = EsObj.newBuilder();
			esObjBuilder.putAllSource(datamap);
			if (hasprimarys) {
				String[] values = new String[primarys.length];
				for (int j = 0; j < values.length; j++) {
					values[j] = datamap.get(primarys[j]);
				}
				String idstr = CollectionUtil.arrayJoin(values, "-");
				if (StringUtils.isEmpty(idstr)) {
					log.error("id是空值");
					continue;
				}
				// 关联关系支持 20181201
				String tablename = dataBuilders.getDump().getTb();
				
				boolean isroot = MappingBean.isRoot(relaObjMap.get(curRule), tablename,curRule.getTbLength());
	            if (isroot) {// 根元素或是没有关联关联的索引
					esObjBuilder.setId(idstr);
					if (relaObjMap.get(curRule) != null && !relaObjMap.get(curRule).isEmpty()) {
						String tableNameTrue = (tablename.length() >= curRule.getTbLength()) ? tablename.substring(0, curRule.getTbLength()) : tablename;
	                    esObjBuilder.setRelaValue(RelaValue.newBuilder().setName(tablename));// tams_relations
					}
				} else {// 有关联关系且不是根元素
					String relaName = MappingBean.getRelaName(relaObjMap.get(curRule), tablename,curRule.getTbLength());
                    String[] relaNameAry = relaName.split(":");
					String parentId = datamap.get(relaNameAry[1]);
					esObjBuilder.setId(String.format("%s:%s", tablename, idstr));// 有可能与主表id相同把主表的ID冲掉
					if (StringUtils.isBlank(parentId)) {// 关联关系没有parent
						errorlog.error(esObjBuilder.toString());// 打错误日志跳过
						continue;
					}
					esObjBuilder.setRelaValue(RelaValue.newBuilder().setName(relaName).setParent(parentId));// tams_relations
				}
			}
			esDataBuilder.addDatas(esObjBuilder.build());
		}

		Result ret = eSClient.docWriteBatch_tc(esDataBuilder.build());
		if (!ret.isSuc()) {
			BulkItemResponse[] retObjs = (BulkItemResponse[]) ret.retObjs();
			for (BulkItemResponse bulkItemResponse : retObjs) {
				if (bulkItemResponse.isFailed()) {
					dataBuilders.getDump().getMetric().counter_send_error.inc();
					log.error(bulkItemResponse.getId());// TODO 错误处理
				}
			}
			LoggerUtil.exit(JvmStatus.s15);
		}
	}

	private Properties configMiddleware(String middlewareType, String middlewareInst) {
		String mergeFolderAndFilePath = IOUtil.mergeFolderAndFilePath(System.getenv("DUCKULA_DATA"),
				String.format("/conf/%s/%s-%s.properties", middlewareType, middlewareType, middlewareInst));
		Properties retProps = IOUtil.fileToProperties(new File(mergeFolderAndFilePath));
		return retProps;
	}

}
