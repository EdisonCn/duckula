package net.wicp.tams.duckula.kafka.consumer.jmx;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.apiext.LoggerUtil;
import net.wicp.tams.common.constant.JvmStatus;
import net.wicp.tams.duckula.kafka.consumer.MainConsumer;

@Data
@Slf4j
public class ConsumerControl implements ConsumerControlMBean {

	private InterProcessMutex lock;

	@Override
	public void stop() {
		log.info("通过MBean服务停止服务");
		// TODO 不在同一线程，肯定出错
		/*
		 * try { lock.release(); } catch (Exception e) { log.error("解锁失败", e); }
		 */
		LoggerUtil.exit(JvmStatus.s15);
	}

	public void setLock(InterProcessMutex lock) {
		this.lock = lock;
	}

	// MainConsumer.metric.counter_send_es
	public long getSendNum() {
		return MainConsumer.metric.counter_send_es.getCount();
	}

	@Override
	public int getTestProp() {
		return 888888;
	}

}
