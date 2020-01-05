package net.wicp.tams.duckula.kafka.consumer.jmx;

public interface ConsumerControlMBean {
	/***
	 * 停止进程
	 */
	public void stop();

	// 得到发送的记录数量
	public long getSendNum();

	// 测试属性
	public int getTestProp();
}
