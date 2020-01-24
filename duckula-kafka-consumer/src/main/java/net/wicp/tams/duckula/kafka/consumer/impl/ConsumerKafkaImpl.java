package net.wicp.tams.duckula.kafka.consumer.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import lombok.extern.slf4j.Slf4j;
import net.wicp.tams.common.Result;
import net.wicp.tams.common.apiext.StringUtil;
import net.wicp.tams.common.others.kafka.KafkaAssitInst;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEvent;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEvent.Builder;
import net.wicp.tams.duckula.client.Protobuf3.OptType;
import net.wicp.tams.duckula.common.beans.Consumer;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.constant.RuleItem;

/***
 * 用于kafka的转换操作， 可以提供全幂等模式的kafka转换
 * 
 * @author andy.zhou
 *
 */
@Slf4j
public class ConsumerKafkaImpl extends ConsumerAbs<DuckulaEvent.Builder> {
	private final KafkaProducer<String, byte[]> producer;
	private final Map<String, Integer> topicPartitionsMap = new HashMap<>();

	public ConsumerKafkaImpl(Consumer consumer) {
		super(consumer);
		producer = KafkaAssitInst.getInst().getKafkaProducer(byte[].class);
	}

	@Override
	public Builder packObj(DuckulaEvent duckulaEvent, Map<String, String> datamap, Rule rule) {
		Builder builder = duckulaEvent.toBuilder();
		if (builder.getOptType() == OptType.delete) {
			builder.putAllBefore(datamap);
		} else {
			builder.putAllAfter(datamap);
		}
		return builder;
	}

	// TODO 发送到下一个topic
	@Override
	public Result doSend(List<Builder> datas) {
		for (Builder data : datas) {
			Rule findReule = findReule(data.getDb(), data.getTb());
			String topic = findReule.getItems().get(RuleItem.topic);// 需要配置要发送的topic
			if (!topicPartitionsMap.containsKey(topic)) {
				List<PartitionInfo> partiList = producer.partitionsFor(topic);
				topicPartitionsMap.put(topic, partiList.size());
			}
			int partitions = topicPartitionsMap.get(topic);
			String keyColName = findReule.getItems().get(RuleItem.key);
			String val = data.getOptType() == OptType.delete ? data.getBeforeMap().get(keyColName)
					: data.getAfterMap().get(keyColName);
			try {
				ProducerRecord<String, byte[]> message = new ProducerRecord<String, byte[]>(topic,
						partitions < 2 ? 0 : StringUtil.partition(val, partitions), val, data.build().toByteArray());

				producer.send(message, new Callback() {
					@Override
					public void onCompletion(RecordMetadata ret, Exception exception) {
						if (exception != null) {// 异常不管，kafka自己有重试机制
							log.error("TimeoutException: Batch Expired,send again:{}", val);
						} else {
							// latch.countDown();
						}
					}
				});
			} catch (Exception e) {
				log.error(String.format("send error,first colvalue:[%s]", val), e);
				throw new IllegalAccessError("发送消息时异常");
			}
		}
		return Result.getSuc();
	}

	// 是否有数据
	@Override
	public boolean checkDataNull(Builder data) {
		return (data.getOptType() == OptType.delete ? data.getBeforeCount() : data.getAfterCount()) == 0;
	}

}
