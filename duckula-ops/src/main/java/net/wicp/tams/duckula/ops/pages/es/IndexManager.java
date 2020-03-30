package net.wicp.tams.duckula.ops.pages.es;

import java.io.IOException;
import java.util.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.gson.JsonParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tapestry5.annotations.Property;
import org.apache.tapestry5.ioc.annotations.Inject;
import org.apache.tapestry5.services.Request;
import org.apache.tapestry5.services.RequestGlobals;
import org.apache.tapestry5.util.TextStreamResponse;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.jdbc.JdbcConnection;
import net.wicp.tams.common.apiext.jdbc.MySqlAssit;
import net.wicp.tams.common.apiext.json.EasyUiAssist;
import net.wicp.tams.common.apiext.json.JSONUtil;
import net.wicp.tams.common.callback.IConvertValue;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.common.web.J2EEAssist;
import net.wicp.tams.component.services.IReq;
import net.wicp.tams.component.tools.TapestryAssist;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.Mapping;
import net.wicp.tams.duckula.common.constant.MiddlewareType;
import net.wicp.tams.duckula.common.constant.OpsPlugEnum;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.ops.beans.DbInstance;
import net.wicp.tams.duckula.ops.servicesBusi.IDuckulaAssit;
import net.wicp.tams.duckula.plugin.IOps;
import net.wicp.tams.duckula.plugin.PluginAssit;
import java.util.Map.Entry;

@Slf4j
public class IndexManager {
	@Inject
	protected RequestGlobals requestGlobals;

	@Inject
	protected Request request;

	@Inject
	private IReq req;
	@Inject
	private IDuckulaAssit duckulaAssit;

	/*
	 * @SessionState(create = false) private ESClient eSClient;
	 *
	 * @SessionState(create = false) private Session session; private boolean
	 * eSClientExists; private boolean sessionExists;
	 */

	private IOps getOps(String cluster) {
		// 获取ES版本
		Properties configMiddleware = PluginAssit.configMiddleware("es", cluster);
		String esName = "es" + StringUtil.hasNull(configMiddleware.getProperty("middleware.version"), "7");
		IOps retops = OpsPlugEnum.valueOf(esName).newOps();
		return retops;
	}

//
//    private static Map<String, ESClient> esclientmap = new HashMap<>();
//
//    public static ESClient getESClient(String cluster) {
//        Validate.isTrue(StringUtil.isNotNull(cluster));
//        if (!esclientmap.containsKey(cluster)) {
//            synchronized (IndexManager.class) {
//                if (!esclientmap.containsKey(cluster)) {
//                    Properties configMiddleware = ConfUtil.configMiddleware(MiddlewareType.es, cluster);
//                    ESClient eSClient = new ESClient(configMiddleware);
//                    esclientmap.put(cluster, eSClient);
//                }
//            }
//        }
//        return esclientmap.get(cluster);
//    }

	@SuppressWarnings("unchecked")
	public TextStreamResponse onQuery(String cluster) {
		// 1.cluster判空
		if (StringUtil.isNull(cluster)) {
			return TapestryAssist.getTextStreamResponse(EasyUiAssist.getJsonForGridEmpty());
		}

		// 2. 入参
		final Mapping mappingparam = TapestryAssist.getBeanFromPage(Mapping.class, requestGlobals);
		String aliasParam = request.getParameter("alias");
		String indexPattern = StringUtil.isNotNull(mappingparam.getIndex()) ? mappingparam.getIndex() : null;

		// 3.结果
		// 3.1.获取ES的索引的昵称
		Set<String> aliases = getOps(cluster).getAliases(cluster, indexPattern, aliasParam);

		// 3.2.获取所有的mappings
		List<Mapping> mappings = getMappings(mappingparam, aliasParam, aliases);

		// 3.3.判断是否已存在(某种字符串格式,需要与俊辉确认)
		IConvertValue<String> existsMap = getExistsMap(cluster, indexPattern);

		// 3.4.获取索引字符串(某种字符串格式,需要与俊辉确认)
		IConvertValue<String> aliasMap = getAliasMap(aliases);

		String retstr = EasyUiAssist.getJsonForGridAlias2(mappings, new String[] { "index,isExit", "index,alias" },
				CollectionUtil.newMap("isExit", existsMap, "alias", aliasMap), mappings.size());
		return TapestryAssist.getTextStreamResponse(retstr);
	}

	@SuppressWarnings("unchecked")
	private List<Mapping> getMappings(Mapping mappingparam, String alias, Set<String> indices) {
		return (List<Mapping>) CollectionUtils.select(ZkUtil.findAllIndex(), object -> {
			Mapping temp = (Mapping) object;
			boolean ret = true;
			if (StringUtil.isNotNull(mappingparam.getIndex())) {
				ret = temp.getId().indexOf(mappingparam.getIndex()) >= 0;
				if (!ret) {
					return false;
				}
			}
			if (StringUtil.isNotNull(mappingparam.getType())) {
				ret = temp.getType().indexOf(mappingparam.getType()) >= 0;
				if (!ret) {
					return false;
				}
			}

			if (StringUtil.isNotNull(alias)) {
				boolean retvalue = indices.contains(temp.getIndex());
				return retvalue;
			}
			return ret;
		});
	}

	private IConvertValue<String> getAliasMap(Set<String> indexSet) {
		return keyObj -> {
			List<String> aliasList = new ArrayList<>();
			for (String alias : indexSet) {
				if (alias.equals(keyObj)) {
					aliasList.add(alias);
				}
			}
			return CollectionUtil.listJoin(aliasList, ",");
		};
	}

	private IConvertValue<String> getExistsMap(String cluster, String indexPattern) {
		return index -> {
			// 重构
			boolean exists = getOps(cluster).isExists(cluster, indexPattern, index);
			return exists ? "存在" : "不存在";

//            String isExit = "不存在";
//            for (IndexBean indexBean : queryIndexs) {
//                if (indexBean.getIndex().equals(keyObj)) {
//                    isExit = "存在";
//                    break;
//                }
//            }
//            return isExit;
		};
	}

	// 用于直接通过连接地址得到索引（无zk信息的索引）
	public TextStreamResponse onQueryIndex(String cluster) {
		String indicesJson = getOps(cluster).getIndicesJson(cluster);
		return TapestryAssist.getTextStreamResponse(indicesJson);
	}

	// 用于级联得到索引（包有zk信息的索引）
	public TextStreamResponse onQueryIndex() {
		if (!request.getParameterNames().contains("parent") || StringUtil.isNull(request.getParameter("parent"))) {
			String retlist = JSONUtil.getJsonForListSimple(null);
			return TapestryAssist.getTextStreamResponse(retlist);
		} else {
			final String parentid = request.getParameter("parent");
			TextStreamResponse onQuery = onQuery(parentid);
			JSONObject parseObject;
			try {
				parseObject = JSONObject.parseObject(IOUtil.slurp(onQuery.getStream()));
			} catch (IOException e) {
				throw new RuntimeException("");
			}
			if (parseObject.containsKey("rows")) {
				JSONArray jsonArray = parseObject.getJSONArray("rows");
				return TapestryAssist.getTextStreamResponse(jsonArray.toJSONString());
			} else {
				String retlist = JSONUtil.getJsonForListSimple(null);
				return TapestryAssist.getTextStreamResponse(retlist);
			}
		}
	}

	/**
	 *
	 * @param middlewareTypeStr
	 * @param version
	 * @return
	 */
	public TextStreamResponse onQueryMiddlewareType(String middlewareTypeStr, String version) {
		if (StringUtil.isNull(middlewareTypeStr)) {
			return TapestryAssist.getTextStreamResponse("[]");
		}
		MiddlewareType middlewareType = MiddlewareType.valueOf(middlewareTypeStr);
		String eleJson = middlewareType.getEleJson(version);
		return TapestryAssist.getTextStreamResponse(eleJson);
	}

	public TextStreamResponse onQueryMiddlewareType(String middlewareTypeStr) {
		return onQueryMiddlewareType(middlewareTypeStr, null);
	}

	public TextStreamResponse onQueryMiddlewareTypeAll() {
		if (!request.getParameterNames().contains("parent")) {
			String retlist = JSONUtil.getJsonForListSimple(null);
			return TapestryAssist.getTextStreamResponse(retlist);
		} else {
			final String parentid = request.getParameter("parent");
			final String version = request.getParameter("version");
			return onQueryMiddlewareType(parentid, version);
		}
	}

	public TextStreamResponse onChangeIndexAlias(String cluster) {
		String oldIndex = request.getParameter("oldIndex");
		String newIndex = request.getParameter("newIndex");
		String[] aliases = request.getParameter("alias").split(",");
		Result retResult = getOps(cluster).renameIndex(cluster, oldIndex, newIndex, aliases);
		return TapestryAssist.getTextStreamResponse(retResult);
	}


	public TextStreamResponse onCreateIndexAlias(String cluster) {
		String oldIndex = request.getParameter("oldIndex");
		String[] aliases = request.getParameter("alias").split(",");
		Result retResult = getOps(cluster).aliasCreate(cluster, oldIndex, aliases);
		return TapestryAssist.getTextStreamResponse(retResult);
	}

    /**
     * 检查索引字段是否符合要求
     * @param index 索引名
     * @param type 类型
     * @param contentJson 当前索引json
     * @return JSONObject
     * 格式：
     * {
     *     add：新增字段,
     *     diff：差异字段
     *     {
     *         oldType:zk存储的字段类型,newType:当前索引字段类型
     *     }
     * }
     */
	private JSONObject checkIndexContent(String index, String type, String contentJson){
		if(StringUtils.isEmpty(index)||StringUtils.isEmpty(type)||StringUtils.isEmpty(contentJson)){
			return new JSONObject();
		}
		String indexPath = String.format("%s/%s",ZkPath.mappings.getRoot(),index+"-"+type);

		//判断结点是否存在
		if(ZkClient.getInst().exists(indexPath)==null){
			return new JSONObject();
		}

		Map<String,String> curTypeMap = JSONObject.parseObject(contentJson,new TypeReference<Map<String,String>>(){});
		JSONObject zkJsonObject = ZkClient.getInst().getZkData(indexPath);
		Map<String,String> oldTypeMap = JSONObject.parseObject(zkJsonObject.getString("content"),
				new TypeReference<Map<String,String>>(){});
		JSONObject diffJosn = new JSONObject();
		JSONObject addJson = new JSONObject();

		//只有新增没有删除
		Iterator curIt = curTypeMap.entrySet().iterator();

		while (curIt.hasNext()){
			Map.Entry entry = (Map.Entry) curIt.next();
			String key = entry.getKey().toString();
			//新增字段
			if(!oldTypeMap.containsKey(key)){
				addJson.put(key,entry.getValue());
			}else{
				String newType = entry.getValue().toString();
				String oldType = oldTypeMap.get(key);
				//差异字段
				if(!StringUtils.equalsAnyIgnoreCase(oldType,newType)){
					JSONObject temp = new JSONObject();
					temp.put("oldType",oldType);
					temp.put("newType",newType);
					diffJosn.put(key,temp);
				}
			}
		}
		JSONObject result = new JSONObject();
		result.put("diff",diffJosn.toString());
		result.put("add",addJson.toString());
		return result;
	}

//	public TextStreamResponse onEditIndex(){
//		final Mapping mappingparam = TapestryAssist.getBeanFromPage(Mapping.class, requestGlobals);
//		JSONObject result = checkIndexContent(mappingparam.getIndex(),mappingparam.getType(),mappingparam.getContent());
//		return TapestryAssist.getTextStreamResponse(result.toJSONString());
//	}

    /**
     * 获取数据库信息并对比zk内容，检查新增字段和差异字段
     * @param paramCluster
     * @return Json数据
     * {
     *   "diff":differenceJson,
     *   "add":addJson
     * }
     */
	public TextStreamResponse onUpdateInitCheck(String paramCluster){
		final Mapping mappingparam = TapestryAssist.getBeanFromPage(Mapping.class, requestGlobals);
		DbInstance temp = ZkClient.getInst().getDateObj(
				String.format("%s/%s", ZkPath.dbinsts.getRoot(), mappingparam.getDbinst()), DbInstance.class);
		java.sql.Connection conn = JdbcConnection.getConnectionMyql(temp.getUrl(), temp.getPort(), temp.getUser(),
				temp.getPwd(), temp.getIsSsh());
		String[][] cols = MySqlAssit.getCols(conn, mappingparam.getDb(), mappingparam.getTb(), YesOrNo.yes);// TODO

		if (StringUtil.isNotNull(mappingparam.getDb1()) && StringUtil.isNotNull(mappingparam.getTb1())) {
			String[][] cols1 = MySqlAssit.getCols(conn, mappingparam.getDb1(), mappingparam.getTb1(), YesOrNo.yes);
			String[] nameAry = CollectionUtil.arrayMerge(String[].class, cols[0], cols1[0]);
			String[] typeAry = CollectionUtil.arrayMerge(String[].class, cols[1], cols1[1]);
			cols = new String[][] { nameAry, typeAry };
		}
		try {
			conn.close();
		} catch (Exception e) {
		}
		String contentjson = "{}";
		if (ArrayUtils.isNotEmpty(cols) && !"_rowkey_".equals(cols[0][0])) {// 有主键
			contentjson = getOps(paramCluster).packIndexContent(cols[0], cols[1], mappingparam.buildRelaNodes());
		}
		//检查新增字段和差异字段
		JSONObject result = checkIndexContent(mappingparam.getIndex(),mappingparam.getType(),contentjson);
		return TapestryAssist.getTextStreamResponse(result.toJSONString());
	}

    /**
     * @param paramCluster
     * @return
     * 返回Json字符串
     * {
     *  "content":contentjson,
     *  "diff":differenceJson,
     *  "add":addJson
     * }
     */
	public TextStreamResponse onCreateIndex(String paramCluster) {
		// String requestPayload =
		// J2EEAssist.getRequestPayload(requestGlobals.getHTTPServletRequest());///////////////////////////////////////////////zjh
		// System.out.println(requestPayload);
		final Mapping mappingparam = TapestryAssist.getBeanFromPage(Mapping.class, requestGlobals);
		// request.

		DbInstance temp = ZkClient.getInst().getDateObj(
				String.format("%s/%s", ZkPath.dbinsts.getRoot(), mappingparam.getDbinst()), DbInstance.class);
		java.sql.Connection conn = JdbcConnection.getConnectionMyql(temp.getUrl(), temp.getPort(), temp.getUser(),
				temp.getPwd(), temp.getIsSsh());
		String[][] cols = MySqlAssit.getCols(conn, mappingparam.getDb(), mappingparam.getTb(), YesOrNo.yes);// TODO

		if (StringUtil.isNotNull(mappingparam.getDb1()) && StringUtil.isNotNull(mappingparam.getTb1())) {
			String[][] cols1 = MySqlAssit.getCols(conn, mappingparam.getDb1(), mappingparam.getTb1(), YesOrNo.yes);
			String[] nameAry = CollectionUtil.arrayMerge(String[].class, cols[0], cols1[0]);
			String[] typeAry = CollectionUtil.arrayMerge(String[].class, cols[1], cols1[1]);
			cols = new String[][] { nameAry, typeAry };
		}
		try {
			conn.close();
		} catch (Exception e) {
		}
		String contentjson = "{}";
		if (ArrayUtils.isNotEmpty(cols) && !"_rowkey_".equals(cols[0][0])) {// 有主键
			contentjson = getOps(paramCluster).packIndexContent(cols[0], cols[1], mappingparam.buildRelaNodes());
		}
		//检查新增字段和差异字段
		JSONObject result = checkIndexContent(mappingparam.getIndex(),mappingparam.getType(),contentjson);
		result.put("content",contentjson);
		return TapestryAssist.getTextStreamResponse(result.toJSONString());
	}

	private boolean checkValidContent(String json){
		String jsonNoWhiteSpace = StringUtils.deleteWhitespace(json);
		return !StringUtils.isEmpty(jsonNoWhiteSpace)&&!StringUtils.equals("{}",jsonNoWhiteSpace);
	}

	public TextStreamResponse onSave(String cluster) {
		final Mapping mappingparam = getMapping();

		int indexOf = mappingparam.getContent().indexOf("\"");
		if (indexOf >= 0) {
			return TapestryAssist.getTextStreamResponse(Result.getError("json内容请使用单引号"));
		}

		String mappingId = mappingparam.getId();
		String index = mappingparam.getIndex();
		String type = mappingparam.getType();
		String content = mappingparam.getContent();
		/**
		 * 修改 net.wicp.tams.duckula.common.beans.Mapping
		 * 新增 addJson,diffJson字段
		 */
		String addJson = mappingparam.getAddJson();
		String diffJson = mappingparam.getDiffJson();
		int shardsNum = mappingparam.getShardsNum();
		int replicas = mappingparam.getReplicas();

		if (StringUtil.isNull(mappingId)) {// 修改
			mappingparam.setId(index + "-" + type);
		}else{
            content = addJson;
		}

		Result createIndex = null;
		try {
			createIndex = getOps(cluster).createIndex(cluster, mappingId, index, type, content, shardsNum, replicas);
		} catch (Exception e) {
			return TapestryAssist.getTextStreamResponse(Result.getError(e.getMessage()));
		}

		if (!createIndex.isSuc()) {
			return TapestryAssist.getTextStreamResponse(createIndex);
		}

        /**
		 * 存在差异字段时修改content
         */
		if(checkValidContent(diffJson)){
			//从zk获取旧json
			JSONObject oldJson = ZkClient.getInst().getZkData(String.format("%s/%s",ZkPath.mappings.getRoot(),index+"-"+type)).getJSONObject("content");
			Set<Entry<String,Object>> addJsonObject = JSON.parseObject(addJson).entrySet();
			for(Entry<String,Object> entry : addJsonObject){
				oldJson.put(entry.getKey(),entry.getValue().toString());
			}
			mappingparam.setContent(JSON.toJSONString(oldJson, SerializerFeature.UseSingleQuotes));
		}

		Result createOrUpdateNode = ZkClient.getInst().createOrUpdateNode(ZkPath.mappings.getPath(mappingparam.getId()),
				JSONObject.toJSONString(mappingparam));
		return TapestryAssist.getTextStreamResponse(createOrUpdateNode);
	}

	private Mapping getMapping() {
		String requestPayload = J2EEAssist.getRequestPayload(requestGlobals.getHTTPServletRequest());/////////////////////////////////////////////// zjh
		System.out.println(requestPayload);

		JSONObject json = JSONUtil.getJsonFromUrlStr(requestPayload);
		final Mapping mappingparam = JSONUtil.getBeanFromJson(Mapping.class, json);

		// final Mapping mappingparam = TapestryAssist.getBeanFromPage(Mapping.class,
		// requestGlobals);
		if (StringUtil.isNull(mappingparam.getContent())) {
			DbInstance temp = ZkClient.getInst().getDateObj(
					String.format("%s/%s", ZkPath.dbinsts.getRoot(), mappingparam.getDbinst()), DbInstance.class);
			java.sql.Connection conn = JdbcConnection.getConnectionMyql(temp.getUrl(), temp.getPort(), temp.getUser(),
					temp.getPwd(), temp.getIsSsh());
			String[][] cols = MySqlAssit.getCols(conn, mappingparam.getDb(), mappingparam.getTb(), YesOrNo.yes);// TODO
			if (StringUtil.isNotNull(mappingparam.getDb1()) && StringUtil.isNotNull(mappingparam.getTb1())) {
				String[][] cols1 = MySqlAssit.getCols(conn, mappingparam.getDb1(), mappingparam.getTb1(), YesOrNo.yes);
				String[] nameAry = CollectionUtil.arrayMerge(String[].class, cols[0], cols1[0]);
				String[] typeAry = CollectionUtil.arrayMerge(String[].class, cols[1], cols1[1]);
				cols = new String[][] { nameAry, typeAry };
			}
			try {
				conn.close();
			} catch (Exception e) {
			}
			String contentjson = getOps(cluster).packIndexContent(cols[0], cols[1], mappingparam.buildRelaNodes());
			mappingparam.setContent(contentjson);
		}
		return mappingparam;
	}

	public TextStreamResponse onDel(String cluster) {
		final Mapping mappingparam = TapestryAssist.getBeanFromPage(Mapping.class, requestGlobals);
		// Result indexDel = getESClient(cluster).indexDel(mappingparam.getIndex());
		// if(!indexDel.isSuc()&&
		// indexDel.getMessage().contains("index_not_found_exception")) {//如果没有找到可以放过
		// indexDel=Result.getSuc();
		// }
		// if (indexDel.isSuc()) {
		ZkUtil.del(ZkPath.mappings, mappingparam.getId());
		// }
		return TapestryAssist.getTextStreamResponse(Result.getSuc());
	}

	@Property
	private String cluster;

	void onActivate(String cluster) {
		this.cluster = cluster;
	}
}
