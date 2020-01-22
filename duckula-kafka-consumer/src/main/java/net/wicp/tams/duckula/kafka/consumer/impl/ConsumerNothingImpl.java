package net.wicp.tams.duckula.kafka.consumer.impl;

import java.util.List;
import java.util.Map;

import net.wicp.tams.common.Result;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEvent;
import net.wicp.tams.duckula.common.beans.Consumer;
import net.wicp.tams.duckula.plugin.beans.Rule;

/***
 * 什么也不做的consumer，主要逻辑写到用户插件中。
 * 
 * @author andy.zhou
 *
 */
public class ConsumerNothingImpl extends ConsumerAbs<byte[]> {

	public ConsumerNothingImpl(Consumer consumer) {
		super(consumer);
	}

	@Override
	public byte[] packObj(DuckulaEvent duckulaEvent, Map<String, String> datamap, Rule rule) {
		return null;
	}

	@Override
	public Result doSend(List<byte[]> datas) {
		return Result.getSuc();
	}

	@Override
	public boolean checkDataNull(byte[] data) {
		return true;
	}

}
