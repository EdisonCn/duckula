package net.wicp.tams.duckula.ops.pages;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.tapestry5.Asset;
import org.apache.tapestry5.annotations.InjectPage;
import org.apache.tapestry5.annotations.OnEvent;
import org.apache.tapestry5.annotations.Path;
import org.apache.tapestry5.annotations.SessionState;
import org.apache.tapestry5.ioc.annotations.Inject;
import org.apache.tapestry5.ioc.internal.util.CollectionFactory;
import org.apache.tapestry5.services.Request;
import org.apache.tapestry5.util.TextStreamResponse;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import net.wicp.tams.common.Conf;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.component.assistbean.Menu;
import net.wicp.tams.component.constant.ResType;
import net.wicp.tams.component.tools.TapestryAssist;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.constant.TaskPattern;
import net.wicp.tams.duckula.ops.beans.SessionBean;

/**
 * Start page of application duckula-ops.
 */
public class Index {

	@Inject
	@Path("context:/menu.properties")
	private Asset asset;

	@SessionState
	private SessionBean sessionBean;

	private boolean sessionBeanExists;

	@InjectPage
	private Login login;

	@Inject
	protected Request request;

	@OnEvent(value = "switchMenu")
	public List<Menu> switchMenu(String moudleId) throws IOException {
		Properties prop = new Properties();
		prop.load(asset.getResource().openStream());
		String[] menus = prop.getProperty("menu.all").split(",");
		List<Menu> retlist = CollectionFactory.newList();
		for (int i = 0; i < menus.length; i++) {
			if (!menus[i].startsWith(moudleId)) {
				continue;
			}
			String id = prop.getProperty(String.format("%s.id", menus[i]));
			TaskPattern curTaskPattern = TaskPattern.getCurTaskPattern();
			if (ArrayUtils.contains(curTaskPattern.getExcludeMenu(), id)) {
				continue;
			}

			String resName = prop.getProperty(String.format("%s.resName", menus[i]));
			String resType = prop.getProperty(String.format("%s.resType", menus[i]));
			String resValue = prop.getProperty(String.format("%s.resValue", menus[i]));
			if (StringUtil.isNotNull(id) && StringUtil.isNotNull(resName) && StringUtil.isNotNull(resType)
					&& StringUtil.isNotNull(resValue)) {
				Menu menu = Menu.builder().id(id).resName(resName).resType(ResType.get(resType)).resValue(resValue)
						.build();
				retlist.add(menu);
			}
		}
		return retlist;
	}

	public TextStreamResponse onDataSave() {
		String saveDataStr = request.getParameter("saveData");
		JSONObject dgAll = JSONObject.parseObject(saveDataStr);
		JSONArray rows = dgAll.getJSONArray("rows");
		System.out.println("rows=" + rows.size());
		ZkClient.getInst().createOrUpdateNode(Conf.get("duckula.zk.rootpath"), saveDataStr);
		return TapestryAssist.getTextStreamResponse(Result.getSuc());
	}

	public TextStreamResponse onDataInit() {
		String zkDataStr = ZkClient.getInst().getZkDataStr(Conf.get("duckula.zk.rootpath"));
		if (StringUtil.isNull(zkDataStr)) {
			zkDataStr = "{\"total\":4,\"rows\":[{\"name\":\"Email\",\"value\":\"***@163.com\",\"group\":\"管理员\",\"editor\":{\"type\":\"validatebox\",\"options\":{\"validType\":\"email\"}}},{\"name\":\"region\",\"value\":\"\",\"group\":\"AWS(亚马逊)\",\"editor\":\"text\"},{\"name\":\"accessKey\",\"value\":\"\",\"group\":\"AWS(亚马逊)\",\"editor\":\"text\"},{\"name\":\"secretKey\",\"value\":\"\",\"group\":\"AWS(亚马逊)\",\"editor\":\"text\"},{\"name\":\"bucketName\",\"value\":\"\",\"group\":\"AWS(亚马逊)\",\"editor\":\"text\"}]}";
		}
		return TapestryAssist.getTextStreamResponse(zkDataStr);
	}

	public Object onActivate() {
		if (!sessionBeanExists || sessionBean == null || sessionBean.getIsLogin() == YesOrNo.no) {
			return login;
		} else {
			return null;
		}
	}
}
