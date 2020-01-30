package net.wicp.tams.duckula.kafka.consumer.impl;

import java.util.List;
import java.util.Map;

import net.wicp.tams.common.Result;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEvent;
import net.wicp.tams.duckula.client.Protobuf3.OptType;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEvent.Builder;
import net.wicp.tams.duckula.common.beans.Consumer;
import net.wicp.tams.duckula.plugin.beans.Rule;

/***
 * 什么也不做的consumer，主要逻辑写到用户插件中。
 * 
 * @author andy.zhou
 *
 */
public class ConsumerNothingImpl extends ConsumerAbs<DuckulaEvent.Builder> {

	public ConsumerNothingImpl(Consumer consumer) {
		super(consumer);
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

	@Override
	public Result doSend(List<Builder> datas) {
		return Result.getSuc();
	}

	@Override
	public boolean checkDataNull(Builder data) {
		return (data.getOptType() == OptType.delete ? data.getBeforeCount() : data.getAfterCount()) == 0;
	}

}
