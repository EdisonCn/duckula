package net.wicp.tams.duckula.task.disruptor;

import com.lmax.disruptor.WorkHandler;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Plugin;
import net.wicp.tams.common.apiext.IOUtil;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.exception.ExceptAll;
import net.wicp.tams.common.exception.ProjectException;
import net.wicp.tams.common.thread.threadlocal.PerThreadValue;
import net.wicp.tams.common.thread.threadlocal.PerthreadManager;
import net.wicp.tams.duckula.plugin.PluginAssit;
import net.wicp.tams.duckula.plugin.busi.BusiAssit;
import net.wicp.tams.duckula.plugin.busi.IBusi;
import net.wicp.tams.duckula.task.Main;
import net.wicp.tams.duckula.task.bean.EventPackage;

@Slf4j
public class DisruptorHandler implements WorkHandler<EventPackage> {

	private final IBusi busi;

	public DisruptorHandler() {
		if (StringUtil.isNotNull(Main.context.getTask().getBusiDowithPluginDir())) {
			Plugin busiPlugin = PluginAssit.newPlugin(
					IOUtil.mergeFolderAndFilePath(DisruptorSendHandler.rootDir.getPath(),
							Main.context.getTask().getBusiDowithPluginDir()),
					"net.wicp.tams.duckula.plugin.busi.IBusi", Thread.currentThread().getContextClassLoader(),
					"net.wicp.tams.duckula.plugin.busi.IBusi");
			Thread.currentThread().setContextClassLoader(busiPlugin.getLoad().getClassLoader());// 需要加载前设置好classload
			// 设置ClientId 20190827
			PerThreadValue<String> createValue = PerthreadManager.getInstance().createValue("duckula-taskId",
					String.class);
			createValue.set(Main.context.getTask().getId());
			// .get(Main.context.getTask().getId());
			this.busi = BusiAssit.loadBusi(busiPlugin);
		} else {
			this.busi = null;
		}
	}

	// private int spitkeyIndex = -1;// 分库分表键的index
	// private int spitkeyType;// 分库分表键的类型

	@Override
	public void onEvent(EventPackage event) throws Exception {
		if (event.getXid() > 0 || busi == null) {
			return;
		}
		event.setOver(false);
		try {
			this.busi.doWith(event, event.getRule());
		} catch (ProjectException e) {
			event.setOver(true);
			if (e.getExcept() != ExceptAll.duckula_nodata) {
				log.error("处理时异常", e);
			} else {
				// 过滤数据
				Main.metric.meter_sender_event_filter.mark(event.getRowsNum());
			}
		} catch (Throwable e) {
			log.error("未知的异常", e);
			event.setOver(true);
		}
	}

}
