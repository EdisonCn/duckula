package net.wicp.tams.duckula.plugin.es6;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Validate;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.jdbc.JdbcConnection;
import net.wicp.tams.common.apiext.jdbc.MySqlAssit;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.common.es.EsAssit;
import net.wicp.tams.common.es.bean.IndexBean;
import net.wicp.tams.common.es.bean.MappingBean;
import net.wicp.tams.common.es.client.ESClient;
import net.wicp.tams.duckula.plugin.IOps;
import net.wicp.tams.duckula.plugin.PluginAssit;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.constant.RuleItem;

@Slf4j
public class OpsPluginImpl implements IOps {

	private static Map<String, ESClient> esclientmap = new HashMap<>();

	private static ESClient getESClient(String cluster) {
		Validate.isTrue(StringUtil.isNotNull(cluster));
		if (!esclientmap.containsKey(cluster)) {
			synchronized (OpsPluginImpl.class) {
				if (!esclientmap.containsKey(cluster)) {
					Properties configMiddleware = PluginAssit.configMiddleware("es", cluster);
					ESClient eSClient = new ESClient(configMiddleware);
					esclientmap.put(cluster, eSClient);
				}
			}
		}
		return esclientmap.get(cluster);
	}
	@Override
	public String createContext(String ip, int port, String user, String pwd,String db,String tb) {
		java.sql.Connection conn = JdbcConnection.getConnectionMyql(ip, port, user, pwd, YesOrNo.no);
		String[][] cols = MySqlAssit.getCols(conn, db, tb, YesOrNo.yes);
		try {
			conn.close();
		} catch (SQLException e1) {
		}
		String contentjson = "";
		if (ArrayUtils.isNotEmpty(cols) && !"_rowkey_".equals(cols[0][0])) {// 有主键
			contentjson = EsAssit.packIndexContent(cols[0], cols[1]);
		}
		return contentjson;
	}

	@Override
	public List<Rule> createIndex(String ruleStr, String cluster, String ip, int port, String user, String pwd) {
		List<Rule> rules = Rule.buildRules(ruleStr);
		List<Rule> retRules=new ArrayList<Rule>();
		for (Rule rule : rules) {
			if (StringUtil.isNotNull(rule.getItems().get(RuleItem.copynum))
					&& StringUtil.isNotNull(rule.getItems().get(RuleItem.partitions))) {
				String db = rule.getDbPattern().replaceAll("\\^", "").replaceAll("\\$", "").replaceAll("\\[0-9\\]\\*",
						"");
				String tb = rule.getTbPattern().replaceAll("\\^", "").replaceAll("\\$", "").replaceAll("\\[0-9\\]\\*",
						"");
				List<IndexBean> queryIndex = getESClient(cluster).queryIndex(rule.getItems().get(RuleItem.index));
				if (CollectionUtils.isEmpty(queryIndex) && !db.endsWith("_") && !tb.endsWith("_")) {
					String contentjson = createContext(ip, port, user, pwd, db, tb);
					if (StringUtil.isNull(contentjson)) {
						continue;
					}
					MappingBean proMappingBean = null;
					try {
						proMappingBean = MappingBean.proMappingBean(contentjson);
					} catch (Exception e) {
					}
					if (proMappingBean == null) {
						continue;
					}
					Result indexCreate = getESClient(cluster).indexCreate(rule.getItems().get(RuleItem.index), "_doc",
							Integer.parseInt(rule.getItems().get(RuleItem.partitions)),
							Integer.parseInt(rule.getItems().get(RuleItem.copynum)), proMappingBean);
					log.info(rule.getItems().get(RuleItem.index) + "创建结果：" + indexCreate.getMessage());
					if (!indexCreate.isSuc()) {
						return retRules;
					}else {
						retRules.add(rule);
					}
				}
			}
		}
		return retRules;
	}

}
