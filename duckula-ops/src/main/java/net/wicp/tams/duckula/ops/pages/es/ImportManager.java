package net.wicp.tams.duckula.ops.pages.es;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.tapestry5.annotations.OnEvent;
import org.apache.tapestry5.annotations.Property;
import org.apache.tapestry5.annotations.SessionState;
import org.apache.tapestry5.ioc.annotations.Inject;
import org.apache.tapestry5.ioc.internal.util.CollectionFactory;
import org.apache.tapestry5.json.JSONArray;
import org.apache.tapestry5.services.Request;
import org.apache.tapestry5.services.RequestGlobals;
import org.apache.tapestry5.util.TextStreamResponse;

import com.alibaba.fastjson.JSONObject;

import common.kubernetes.constant.ResourcesType;
import common.kubernetes.tiller.TillerClient;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Conf;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.CollectionUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.json.EasyUiAssist;
import net.wicp.tams.common.callback.IConvertValue;
import net.wicp.tams.component.annotation.HtmlJs;
import net.wicp.tams.component.constant.EasyUIAdd;
import net.wicp.tams.component.services.IReq;
import net.wicp.tams.component.tools.TapestryAssist;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.Dump;
import net.wicp.tams.duckula.common.constant.CommandType;
import net.wicp.tams.duckula.common.constant.TaskPattern;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.ops.beans.Server;
import net.wicp.tams.duckula.ops.servicesBusi.IDuckulaAssit;

@Slf4j
@HtmlJs(easyuiadd = { EasyUIAdd.edatagrid })
public class ImportManager {
	@Inject
	protected RequestGlobals requestGlobals;

	@Inject
	protected Request request;

	@Inject
	private IReq req;
	@Inject
	private IDuckulaAssit duckulaAssit;

	@SessionState
	@Property
	private String namespace;

	private boolean namespaceExists;

	public boolean isNeedServer() {
		return TaskPattern.isNeedServer();
	}

	public String getDefaultNamespace() {
		String namespaceTrue = "all".equalsIgnoreCase(namespace) ? "" : namespace;
		return StringUtil.hasNull(namespaceTrue, Conf.get("common.kubernetes.apiserver.namespace.default"));
	}

	public String getDefaultImageVersion() {
		return Conf.get("duckula.task.image.tag");
	}

	public String getColDifferent() {
		if (isNeedServer()) {
			return "{field:'hosts',width:100,title:'任务主机'}";
		} else {
			return "{field:'podStatus',width:120,title:'k8s状态'},{field:'imageVersion',width:80,title:'image版本'},{field:'namespace',width:100,title:'名称空间'}";
		}
	}

	@SuppressWarnings("unchecked")
	public TextStreamResponse onQuery() {
		if (!namespaceExists) {
			String jsonStr = EasyUiAssist.getJsonForGridEmpty();
			return TapestryAssist.getTextStreamResponse(jsonStr);
		}
		final Dump dumpparam = TapestryAssist.getBeanFromPage(Dump.class, requestGlobals);
		List<Dump> dumps = ZkUtil.findAllDump();
		List<Dump> retlist = (List<Dump>) CollectionUtils.select(dumps, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				Dump temp = (Dump) object;
				if (!TaskPattern.isNeedServer() && !"all".equals(namespace)
						&& !namespace.equalsIgnoreCase(temp.getNamespace())) {
					return false;
				}

				boolean ret = true;
				if (StringUtil.isNotNull(dumpparam.getId())) {
					ret = temp.getId().indexOf(dumpparam.getId()) >= 0;
					if (!ret) {
						return false;
					}
				}

				if (StringUtil.isNotNull(dumpparam.getDbinst())) {
					ret = temp.getDbinst().indexOf(dumpparam.getDbinst()) >= 0;
					if (!ret) {
						return false;
					}
				}
				return ret;
			}
		});

		String retstr = null;
		if (isNeedServer()) {
			final Map<String, List<String>> taskRunServerMap = new HashMap<>();
			List<Server> findAllServers = duckulaAssit.findAllServers();
			for (Dump dump : retlist) {
				List<String> serverids = duckulaAssit.lockToServer(findAllServers, ZkPath.dumps, dump.getId());
				taskRunServerMap.put(dump.getId(), serverids);
			}
			IConvertValue<String> hostNumConvert = new IConvertValue<String>() {
				@Override
				public String getStr(String keyObj) {
					return String.valueOf(taskRunServerMap.get(keyObj).size());
				}
			};

			IConvertValue<String> hostNumList = new IConvertValue<String>() {
				@Override
				public String getStr(String keyObj) {
					return CollectionUtil.listJoin(taskRunServerMap.get(keyObj), ",");
				}
			};

			Map<String, IConvertValue<String>> convertsMap = new HashMap<>();
			convertsMap.put("hostNum", hostNumConvert);
			convertsMap.put("hosts", hostNumList);

			retstr = EasyUiAssist.getJsonForGridAlias(retlist, new String[] { "id,hostNum", "id,hosts" }, convertsMap,
					retlist.size());
		} else {
			IConvertValue<String> podStatus = new IConvertValue<String>() {
				@Override
				public String getStr(String keyObj) {
					keyObj = CommandType.dump.getK8sId(keyObj);
					Map<ResourcesType, String> queryStatus = TillerClient.getInst().queryStatus(keyObj);
					String valueStr = queryStatus.get(ResourcesType.Pod);
					String colValue = ResourcesType.Pod.getColValue(valueStr, "STATUS");
					return colValue;
				}
			};
			Map<String, IConvertValue<String>> convertsMap = new HashMap<>();
			convertsMap.put("podStatus", podStatus);
			retstr = EasyUiAssist.getJsonForGridAlias(retlist, new String[] { "id,podStatus" }, convertsMap,
					retlist.size());// .getJsonForGridAlias(retlist, retlist.size());
		}
		return TapestryAssist.getTextStreamResponse(retstr);
	}

	@SuppressWarnings("unchecked")
	@OnEvent(value = "savesel")
	private Result sava(JSONArray selIds, org.apache.tapestry5.json.JSONObject paramsObj) {
		long curtime1 = new Date().getTime();
		String idstr = paramsObj.has("id") ? paramsObj.getString("id") : null;// 旧的已启动的服务
		String taskid = paramsObj.getString("taskid");
		String[] ipsAry = StringUtil.isNotNull(idstr) ? idstr.split(",") : new String[0];
		final List<String> ips = CollectionFactory.newList();// 旧的已启动的服务
		for (int i = 0; i < ipsAry.length; i++) {
			if (StringUtil.isNotNull(ipsAry[i])) {
				ips.add(ipsAry[i].split("\\|")[0]);
			}
		}
		final List<Object> idsneed = selIds.toList();// 已选择的ip
		List<String> adds = (List<String>) CollectionUtils.select(idsneed, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				return !ips.contains(object);
			}
		});

		List<String> dels = (List<String>) CollectionUtils.select(ips, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				return !idsneed.contains(object);
			}
		});

		try {
			List<Server> allserver = duckulaAssit.findAllServers();
			StringBuffer errmsg = new StringBuffer();
			if (CollectionUtils.isNotEmpty(dels)) {// stop
				for (String del : dels) {
					Server curserver = selServer(allserver, del);
					Result ret = duckulaAssit.stopTask(CommandType.dump, taskid, curserver, false);
					if (!ret.isSuc()) {
						errmsg.append(ret.getMessage());
					}
				}
			}
			if (CollectionUtils.isNotEmpty(adds)) {// start
				for (String add : adds) {
					Server curserver = selServer(allserver, add);
					Result ret = duckulaAssit.startTask(CommandType.dump, taskid, curserver, false);
					if (!ret.isSuc()) {
						errmsg.append(ret.getMessage());
					}
				}
			}
			if (errmsg.length() > 0) {
				return Result.getError(errmsg.toString());
			}
		} catch (Exception e) {
			return Result.getError("出错:" + e.getMessage());
		}

		// 等待一段时间，为启动各个task留点时间
		long curtime2 = System.currentTimeMillis();
		while ((curtime2 - curtime1) < 3000) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			curtime2 = new Date().getTime();
		}
		return Result.getSuc();
	}

	private Server selServer(List<Server> allserver, String serverid) {
		if (CollectionUtils.isEmpty(allserver)) {
			return null;
		}
		for (Server server : allserver) {
			if (server.getIp().equals(serverid)) {
				return server;
			}
		}
		return null;
	}

	public TextStreamResponse onStartK8sTask() {
		long curtime1 = new Date().getTime();
		String taskid = request.getParameter("taskid");
		Result ret = duckulaAssit.startTaskForK8s(CommandType.dump, taskid, true);// TODO pvc的初始化需解决、可以传入参数standalone
		// 等待一段时间，为启动各个task留点时间
		long curtime2 = System.currentTimeMillis();
		while ((curtime2 - curtime1) < 3000) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			curtime2 = new Date().getTime();
		}
		return TapestryAssist.getTextStreamResponse(ret);
	}

	public TextStreamResponse onSave() {
		final Dump dumpparam = TapestryAssist.getBeanFromPage(Dump.class, requestGlobals);
		if (StringUtil.isNull(dumpparam.getId())) {
		}
		// String[] split = dumpparam.getDb_tb().split("\\.");
		// dumpparam.setPrimarys(primarys);
		Result createOrUpdateNode = ZkClient.getInst().createOrUpdateNode(ZkPath.dumps.getPath(dumpparam.getId()),
				JSONObject.toJSONString(dumpparam));
		return TapestryAssist.getTextStreamResponse(createOrUpdateNode);
	}

	public TextStreamResponse onDel() {
		String dumpId = request.getParameter("id");
		Result del = ZkUtil.del(ZkPath.dumps, dumpId);
		return TapestryAssist.getTextStreamResponse(del);
	}

	public void onActivate(String namespace) {
		this.namespace = namespace;
	}
}
