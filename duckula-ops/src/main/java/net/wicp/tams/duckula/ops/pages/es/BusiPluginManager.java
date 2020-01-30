package net.wicp.tams.duckula.ops.pages.es;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.io.FileUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.tapestry5.SymbolConstants;
import org.apache.tapestry5.annotations.Property;
import org.apache.tapestry5.ioc.annotations.Inject;
import org.apache.tapestry5.ioc.annotations.Symbol;
import org.apache.tapestry5.services.Request;
import org.apache.tapestry5.services.RequestGlobals;
import org.apache.tapestry5.upload.services.MultipartDecoder;
import org.apache.tapestry5.util.TextStreamResponse;

import com.alibaba.fastjson.JSONObject;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.apiext.json.EasyUiAssist;
import net.wicp.tams.common.callback.IConvertValue;
import net.wicp.tams.common.callback.impl.convertvalue.ConvertValueEnum;
import net.wicp.tams.common.constant.DateFormatCase;
import net.wicp.tams.common.constant.dic.YesOrNo;
import net.wicp.tams.common.os.bean.FileBean;
import net.wicp.tams.common.os.pool.SSHConnection;
import net.wicp.tams.component.services.IReq;
import net.wicp.tams.component.tools.TapestryAssist;
import net.wicp.tams.duckula.common.ConfUtil;
import net.wicp.tams.duckula.common.ZkClient;
import net.wicp.tams.duckula.common.ZkUtil;
import net.wicp.tams.duckula.common.beans.BusiPlugin;
import net.wicp.tams.duckula.common.constant.PluginType;
import net.wicp.tams.duckula.common.constant.TaskPattern;
import net.wicp.tams.duckula.common.constant.ZkPath;
import net.wicp.tams.duckula.ops.beans.Server;
import net.wicp.tams.duckula.ops.servicesBusi.DuckulaUtils;
import net.wicp.tams.duckula.ops.servicesBusi.IDuckulaAssit;

@Slf4j
public class BusiPluginManager {
	@Inject
	protected RequestGlobals requestGlobals;

	@Inject
	protected Request request;

	@Inject
	private IReq req;
	@Inject
	private IDuckulaAssit duckulaAssit;

	@Inject
	private MultipartDecoder decoder;

	@Property
	@Inject
	@Symbol(SymbolConstants.CONTEXT_PATH)
	private String contextPath;

	public boolean isNeedServer() {
		return TaskPattern.isNeedServer();
	}

	public String getColDifferent() {
		if (isNeedServer()) {
			return "{field:'fileExitServer',width:100,title:'同步状态',formatter:showstatus},";
		} else {
			return "{field:'fileExitServer',width:100,title:'同步状态',formatter:showstatus},";
		}
	}

	@SuppressWarnings("unchecked")
	public TextStreamResponse onQuery() {
		Map<PluginType, List<String>> tempmap = new HashMap<PluginType, List<String>>();
		for (PluginType pluginType : PluginType.values()) {
			List<String> templist = tempmap.get(pluginType);
			if (templist == null) {
				templist = new ArrayList<String>();
				tempmap.put(pluginType, templist);
			}
			String pluginpath = pluginType.getPluginDir(false);
			File pluginDir = new File(pluginpath);
			if (!pluginDir.exists()) {
				try {
					FileUtils.forceMkdir(pluginDir);
				} catch (Exception e) {
					log.error("创建用户插件目录失败", e);
				}
			} else {
				File[] files = pluginDir.listFiles();
				for (File file : files) {
					if (!file.isDirectory()) {
						templist.add(file.getName());
					}
				}
			}
		}
		IConvertValue<Object> fileExit = new IConvertValue<Object>() {
			@Override
			public String getStr(Object keyObj) {
				BusiPlugin opsPlug = (BusiPlugin) keyObj;
				if (!tempmap.get(opsPlug.getPluginType()).contains(opsPlug.getPluginFileName())) {
					return "否";
				} else {
					return "是";
				}
			}
		};

		final BusiPlugin busiPluginQuery = TapestryAssist.getBeanFromPage(BusiPlugin.class, requestGlobals);
		List<BusiPlugin> plugins = ZkUtil.findAllObjs(ZkPath.busiplugins, BusiPlugin.class);
		List<BusiPlugin> retlist = (List<BusiPlugin>) CollectionUtils.select(plugins, new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				BusiPlugin temp = (BusiPlugin) object;
				boolean ret = true;
				if (StringUtil.isNotNull(busiPluginQuery.getProjectName())) {
					ret = temp.getProjectName().indexOf(busiPluginQuery.getProjectName()) >= 0;
					if (!ret) {
						return false;
					}
				}
				return ret;
			}
		});

		String retstr = null;
		if (isNeedServer()) {
			Map<PluginType, Map<String, List<FileBean>>> serverplugs = new HashMap<>();
			List<Server> servers = ZkUtil.findAllObjs(ZkPath.servers, Server.class);
			for (Server server : servers) {
				if ("localhost".equals(server.getIp())) {
					continue;
				}
				if (server.getIsInit() == YesOrNo.yes) {
					SSHConnection conn = DuckulaUtils.getConn(server);
					for (PluginType pluginType : PluginType.values()) {
						List<FileBean> llFiles = conn.llFile(pluginType.getPluginDir(true), YesOrNo.no);
						Map<String, List<FileBean>> iptopluginsmap = serverplugs.get(server.getIp());
						if (iptopluginsmap == null) {
							iptopluginsmap = new HashMap<String, List<FileBean>>();
							serverplugs.put(pluginType, iptopluginsmap);
						}
						iptopluginsmap.put(server.getIp(), llFiles);
					}
					DuckulaUtils.returnConn(server, conn);
				}
			}

			IConvertValue<Object> fileExitServer = new IConvertValue<Object>() {
				@Override
				public String getStr(Object keyObj) {// 得到Id
					BusiPlugin opsPlug = (BusiPlugin) keyObj;
					// String ret = "0";// -1:有异常，需要处理 0：不存在 1:存在但需要更新 2:存在无需更新
					for (Server server : servers) {
						if ("localhost".equals(server.getIp())) {
							continue;
						}
						List<FileBean> list = serverplugs.get(opsPlug.getPluginType()).get(server.getIp());
						if (list == null) {
							return "-1";// 服务器上插件不存在
						}
						boolean isExit = false;
						for (FileBean busiPlugin : list) {
							if (busiPlugin.getFileName().equals(opsPlug.getPluginFileName().replace(".tar", ""))) {
								try {
									Date opsDate = DateFormatCase.YYYY_MM_DD_hhmmss.getInstanc()
											.parse(opsPlug.getLastUpdateTime());
									if (opsDate.after(busiPlugin.getLastUpdateTime())) {
										return "1";// 插件较旧，需要更新
									}
									isExit = true;
									break;
								} catch (Exception e) {
									log.error("插件更新时间比较失败", e);
									return "-1";// 对比插件时异常
								}
							}
						}
						if (!isExit) {
							return "0";// 没有要找的插件
						}
					}
					return "2";// 插件正常
				}
			};

			retstr = EasyUiAssist.getJsonForGrid(retlist,
					new String[] { "id", "projectName", "pluginType", "pluginType", "update", "lastUpdateTime",
							"pluginFileName", ",fileExit", ",fileExitServer", "pluginType,pluginType1" },
					new IConvertValue[] { null, null, null, null, null, null, fileExit, fileExitServer,
							new ConvertValueEnum(PluginType.class) },
					retlist.size());
		} else {
			JSONObject configGlobal = ConfUtil.getConfigGlobal();

			AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(configGlobal.getString("region")).withCredentials(
					new AWSStaticCredentialsProvider(new BasicAWSCredentials(configGlobal.getString("accessKey"),
							configGlobal.getString("secretKey"))))
					.build();
			ObjectListing listObjects = s3.listObjects(configGlobal.getString("bucketName"), PluginType.getRootpath());
			Map<PluginType, List<S3ObjectSummary>> serverplugs = new HashMap<>();
			for (S3ObjectSummary s3ObjectSummary : listObjects.getObjectSummaries()) {
				String key = s3ObjectSummary.getKey();
				for (PluginType pluginType : PluginType.values()) {
					if (key.startsWith(pluginType.getPluginDirKey())) {
						List<S3ObjectSummary> templist = serverplugs.get(pluginType);
						if (templist == null) {
							templist = new ArrayList<S3ObjectSummary>();
							serverplugs.put(pluginType, templist);
						}
						templist.add(s3ObjectSummary);
						break;
					}
				}
			}
			IConvertValue<Object> fileExitServer = new IConvertValue<Object>() {
				@Override
				public String getStr(Object keyObj) {// 得到Id
					BusiPlugin opsPlug = (BusiPlugin) keyObj;
					// String ret = "0";// -1:有异常，需要处理 0：不存在 1:存在但需要更新 2:存在无需更新
					List<S3ObjectSummary> list = serverplugs.get(opsPlug.getPluginType());
					boolean isExit = false;
					for (S3ObjectSummary s3ObjectSummary : list) {
						if (s3ObjectSummary.getKey().startsWith(opsPlug.getPluginType().getPluginDirKey())) {
							try {
								Date opsDate = DateFormatCase.YYYY_MM_DD_hhmmss.getInstanc()
										.parse(opsPlug.getLastUpdateTime());
								if (opsDate.after(s3ObjectSummary.getLastModified())) {
									return "1";// 插件较旧，需要更新
								}
								isExit = true;
								break;
							} catch (Exception e) {
								log.error("插件更新时间比较失败", e);
								return "-1";// 对比插件时异常
							}
						}
					}
					if (!isExit) {
						return "0";// 没有要找的插件
					} else {
						return "2";// 插件正常
					}
				}
			};
			retstr = EasyUiAssist.getJsonForGrid(retlist,
					new String[] { "id", "projectName", "pluginType", "update", "lastUpdateTime", "pluginFileName",
							",fileExit", ",fileExitServer", "pluginType,pluginType1" },
					new IConvertValue[] { null, null, null, null, null, null, fileExit, fileExitServer,
							new ConvertValueEnum(PluginType.class) },
					retlist.size());
		}
		return TapestryAssist.getTextStreamResponse(retstr);
	}

	public TextStreamResponse onSave() {
		final BusiPlugin busiPlugin = TapestryAssist.getBeanFromPage(BusiPlugin.class, requestGlobals);
		busiPlugin.setLastUpdateTime(DateFormatCase.YYYY_MM_DD_hhmmss.getInstanc().format(new Date()));
		Result createOrUpdateNode = ZkClient.getInst().createOrUpdateNode(
				ZkPath.busiplugins.getPath(busiPlugin.getId()), JSONObject.toJSONString(busiPlugin));
		return TapestryAssist.getTextStreamResponse(createOrUpdateNode);
	}

	public void onSaveFile() {
		String id = request.getParameter("id");
		BusiPlugin busiPlugin = JSONObject.toJavaObject(ZkClient.getInst().getZkData(ZkPath.busiplugins.getPath(id)),
				BusiPlugin.class);
		String pluginpath = busiPlugin.getPluginType().getPluginDir(false);
		File pluginDir = new File(pluginpath);
		List<File> uploadlist = req.saveUpload(pluginDir);// 新文件会覆盖旧文件
		File pluginFile = uploadlist.get(0);
		busiPlugin.setPluginFileName(pluginFile.getName());
		busiPlugin.setLastUpdateTime(DateFormatCase.YYYY_MM_DD_hhmmss.getInstanc().format(new Date()));
		ZkClient.getInst().createOrUpdateNode(ZkPath.busiplugins.getPath(busiPlugin.getId()),
				JSONObject.toJSONString(busiPlugin));
	}

	public TextStreamResponse onUploadPlug() throws ClientProtocolException, IOException {
		final BusiPlugin busiPluginSyc = TapestryAssist.getBeanFromPage(BusiPlugin.class, requestGlobals);
		String plugFilePath = IOUtil.mergeFolderAndFilePath(busiPluginSyc.getPluginType().getPluginDir(false),
				busiPluginSyc.getPluginFileName());
		if (!TaskPattern.isNeedServer()) {// k8s版本需要上传文件到s3或oss
			JSONObject configGlobal = ConfUtil.getConfigGlobal();
			AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(configGlobal.getString("region")).withCredentials(
					new AWSStaticCredentialsProvider(new BasicAWSCredentials(configGlobal.getString("accessKey"),
							configGlobal.getString("secretKey"))))
					.build();
			String plugindir = IOUtil.mergeFolderAndFilePath(busiPluginSyc.getPluginType().getPluginDirKey(),
					busiPluginSyc.getPluginFileName());
			PutObjectResult putObject = s3.putObject(configGlobal.getString("bucketName"), plugindir,
					new File(plugFilePath));
			return TapestryAssist.getTextStreamResponse(Result.getSuc(putObject.getETag()));
		} else {
			List<Server> servers = ZkUtil.findAllObjs(ZkPath.servers, Server.class);
			try {
				for (Server server : servers) {
					if ("localhost".equals(server.getIp())) {
						continue;
					}
					SSHConnection conn = DuckulaUtils.getConn(server);
					conn.scpToDir(plugFilePath, busiPluginSyc.getPluginType().getPluginDir(true));
					String remoteFile = IOUtil.mergeFolderAndFilePath(busiPluginSyc.getPluginType().getPluginDir(true),
							busiPluginSyc.getPluginFileName());
					conn.tarX(remoteFile);
					DuckulaUtils.returnConn(server, conn);
				}
				return TapestryAssist.getTextStreamResponse(Result.getSuc());
			} catch (Exception e) {
				return TapestryAssist.getTextStreamResponse(Result.getError(e.getMessage()));
			}
		}
	}

	public TextStreamResponse onDel() {
		final BusiPlugin busiPlugin = TapestryAssist.getBeanFromPage(BusiPlugin.class, requestGlobals);
		Result del = ZkUtil.del(ZkPath.busiplugins, busiPlugin.getId());
		return TapestryAssist.getTextStreamResponse(del);
	}
}
