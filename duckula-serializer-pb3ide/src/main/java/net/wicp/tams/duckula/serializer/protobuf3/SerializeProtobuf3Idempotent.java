package net.wicp.tams.duckula.serializer.protobuf3;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;

import net.wicp.tams.common.apiext.ReflectAssist;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.json.JSONUtil;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEventIdempotent;
import net.wicp.tams.duckula.client.Protobuf3.IdempotentEle;
import net.wicp.tams.duckula.client.Protobuf3.OptType;
import net.wicp.tams.duckula.plugin.beans.DuckulaPackage;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.beans.SingleRecord;
import net.wicp.tams.duckula.plugin.constant.RuleItem;
import net.wicp.tams.duckula.plugin.serializer.ISerializer;

public class SerializeProtobuf3Idempotent implements ISerializer {

	@Override
	public List<SingleRecord> serialize(DuckulaPackage duckulaPackage, String splitKey) {
		DuckulaEventIdempotent.Builder build = DuckulaEventIdempotent.newBuilder();
		build.setDb(duckulaPackage.getEventTable().getDb());
		build.setTb(duckulaPackage.getEventTable().getTb());
		OptType optType = OptType.forNumber(duckulaPackage.getEventTable().getOptType().getValue());
		build.setOptType(optType);
		build.setCommitTime(duckulaPackage.getEventTable().getCommitTime());

		String[][] orivalues = optType == OptType.delete ? duckulaPackage.getBefores() : duckulaPackage.getAfters();
		String[] cols = duckulaPackage.getEventTable().getCols();
		int[] colsType = duckulaPackage.getEventTable().getColsType();
		int keyIndex[] = new int[] { 0 };// 默认为第一个列为主键
		if (StringUtil.isNotNull(splitKey)) {
			String[] keyCols = splitKey.split(",");
			keyIndex = new int[keyCols.length];
			for (int i = 0; i < keyCols.length; i++) {
				keyIndex[i] = ArrayUtils.indexOf(cols, keyCols[i]);
				build.addKeyNames(cols[keyIndex[i]]);// 设置keyName
				build.addKeyTypesValue(colsType[keyIndex[i]]);// 添加类型
			}
		} else {
			build.addKeyNames(cols[0]);// 默认为第一个列为主键
			build.addKeyTypesValue(colsType[0]);
		}
		//处理附加列值
		int[] colIndexs =null;
		Rule rule = duckulaPackage.getRule();
		String[] colNames =  StringUtil.isNull(rule.getItems().get(RuleItem.colName))?null:rule.getItems().get(RuleItem.colName).split(",");
		if(ArrayUtils.isNotEmpty(colNames)) {
			colIndexs=new int[colNames.length];
			for (int i = 0; i < colNames.length; i++) {
				colIndexs[i] = ArrayUtils.indexOf(cols, colNames[i]);	
				if(colIndexs[i]==-1) {
					System.out.println("error");
				}
				build.addAddColNames(cols[colIndexs[i]]);// 设置keyName
				build.addAddColTypesValue(colsType[colIndexs[i]]);// 添加类型
			}
		}
		
		//处理附加属性（固定值）
		if(StringUtil.isNotNull(rule.getItems().get(RuleItem.addProp))) {
			build.setAddProp(rule.getItems().get(RuleItem.addProp));
		}
		
		
		for (int i = 0; i < duckulaPackage.getRowsNum(); i++) {
			IdempotentEle.Builder rowbuilder = IdempotentEle.newBuilder();
			for (int j = 0; j < keyIndex.length; j++) {
				rowbuilder.addKeyValues(orivalues[i][keyIndex[j]]);
			}
			if(colIndexs!=null) {
				for (int j = 0; j < colIndexs.length; j++) {
					rowbuilder.addAddColValues(orivalues[i][colIndexs[j]]);
				}
			}
			build.addValues(rowbuilder);
		}
		List<SingleRecord> retlist = new ArrayList<SingleRecord>();
		SingleRecord addele = SingleRecord.builder().db(duckulaPackage.getEventTable().getDb())
				.tb(duckulaPackage.getEventTable().getTb()).optType(duckulaPackage.getEventTable().getOptType())
				.data(build.build().toByteArray()).build();
		retlist.add(addele);
		return retlist;
	}

}
