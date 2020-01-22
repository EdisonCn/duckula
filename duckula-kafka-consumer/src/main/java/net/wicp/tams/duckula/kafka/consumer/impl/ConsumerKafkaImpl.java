package net.wicp.tams.duckula.kafka.consumer.impl;

import java.util.List;
import java.util.Map;

import net.wicp.tams.common.Result;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEvent;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEvent.Builder;
import net.wicp.tams.duckula.client.Protobuf3.OptType;
import net.wicp.tams.duckula.common.beans.Consumer;
import net.wicp.tams.duckula.plugin.beans.Rule;

/***
 * 用于kafka的转换操作， 可以提供全幂等模式的kafka转换
 * 
 * @author andy.zhou
 *
 */
public class ConsumerKafkaImpl extends ConsumerAbs<DuckulaEvent.Builder> {

	public ConsumerKafkaImpl(Consumer consumer) {
		super(consumer);
	}

	@Override
	public Builder packObj(DuckulaEvent duckulaEvent, Map<String, String> datamap, Rule rule) {
		return duckulaEvent.toBuilder();
	}

	// TODO 发送到下一个topic
	@Override
	public Result doSend(List<Builder> datas) {
		// TODO Auto-generated method stub
		return null;
	}

//是否有数据
	@Override
	public boolean checkDataNull(Builder data) {
		return (data.getOptType() == OptType.delete ? data.getBeforeCount() : data.getAfterCount()) == 0;
	}

}
