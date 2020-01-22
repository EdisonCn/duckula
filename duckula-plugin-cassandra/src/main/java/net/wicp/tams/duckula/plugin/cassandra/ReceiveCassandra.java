package net.wicp.tams.duckula.plugin.cassandra;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.Validate;

import com.alibaba.fastjson.JSONObject;

import net.wicp.tams.common.Conf;
import net.wicp.tams.common.cassandra.CassandraAssit;
import net.wicp.tams.common.cassandra.bean.Columns;
import net.wicp.tams.common.cassandra.jdbc.CassandraData;
import net.wicp.tams.common.cassandra.jdbc.CassandraDatas;
import net.wicp.tams.common.cassandra.jdbc.CassandraDatas.Builder;
import net.wicp.tams.common.cassandra.jdbc.OptType;
import net.wicp.tams.common.exception.ExceptAll;
import net.wicp.tams.common.exception.ProjectExceptionRuntime;
import net.wicp.tams.duckula.plugin.beans.DuckulaPackage;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.beans.SingleRecord;
import net.wicp.tams.duckula.plugin.constant.RuleItem;
import net.wicp.tams.duckula.plugin.receiver.ReceiveAbs;

public class ReceiveCassandra extends ReceiveAbs {

	public ReceiveCassandra(JSONObject paramObjs) {
		super(paramObjs);
		Conf.overProp(props);
	}

	@Override
	public boolean receiveMsg(List<SingleRecord> data, Rule rule) {
		throw new ProjectExceptionRuntime(ExceptAll.project_nosupport, "ES接收者不支持序列化，序列化请配置成‘无’");
	}

	@Override
	public boolean isSync() {
		return true;
	}

	private Map<String, Map<String, String>> typemap = new HashMap<String, Map<String, String>>();

//缓存一下类型
	private Map<String, String> getType(String ks, String table) {
		String keystr = ks + "-" + table;
		if (typemap.get(keystr) == null) {// 不用双重检查了，再执行一次也关系不大
			List<Columns> queryCols = CassandraAssit.queryCols(ks, table);
			Map<String, String> retmap = new HashMap<String, String>();
			for (Columns columns : queryCols) {
				retmap.put(columns.getColumnName(), columns.getType());
			}
			typemap.put(keystr, retmap);
		}
		return typemap.get(keystr);
	}

	@Override
	public boolean receiveMsg(DuckulaPackage duckulaPackage, Rule rule, String splitKey) {
		String idKey = rule.getItems().get(RuleItem.key) == null ? duckulaPackage.getEventTable().getCols()[0]
				: rule.getItems().get(RuleItem.key);
		String ks = rule.getItems().get(RuleItem.ks);
		String table = rule.getItems().get(RuleItem.table);
		Validate.notBlank(ks);
		Validate.notBlank(table);
		String[][] datas = (duckulaPackage.getEventTable().getOptType() == net.wicp.tams.common.constant.OptType.delete)
				? duckulaPackage.getBefores()
				: duckulaPackage.getAfters();

		Builder builder = CassandraDatas.newBuilder();
		builder.setKs(ks);
		builder.setTb(table);
		builder.setKey(idKey);
		Map<String, String> types = getType(ks, table);
		for (String typekey : types.keySet()) {
			builder.putType(typekey, types.get(typekey));
		}
		for (String[] data : datas) {
			CassandraData.Builder tempData = CassandraData.newBuilder();
			switch (duckulaPackage.getEventTable().getOptType()) {
			case delete:
				tempData.setOptType(OptType.delete);
				break;
			case update:
				tempData.setOptType(OptType.update);
				break;
			case insert:
				tempData.setOptType(OptType.insert);
				break;
			default:
				break;
			}
			for (int i = 0; i < data.length; i++) {
				String key = duckulaPackage.getEventTable().getCols()[i];
				String value = data[i];
				if (value != null) {
					tempData.putValue(key, value);
				}
			}
			builder.addDatas(tempData);
		}
		CassandraAssit.optDatas(builder.build());
		return true;
	}

}
