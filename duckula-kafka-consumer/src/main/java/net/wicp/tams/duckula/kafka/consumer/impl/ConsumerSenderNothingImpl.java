package net.wicp.tams.duckula.kafka.consumer.impl;

import com.alibaba.fastjson.JSONObject;
import net.wicp.tams.common.Result;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEvent;
import net.wicp.tams.duckula.client.Protobuf3.DuckulaEvent.Builder;
import net.wicp.tams.duckula.client.Protobuf3.OptType;
import net.wicp.tams.duckula.plugin.beans.Rule;
import net.wicp.tams.duckula.plugin.receiver.consumer.ConsumerSenderAbs;

import java.util.List;
import java.util.Map;

/***
 * 什么也不做的consumer，主要逻辑写到用户插件中。
 *
 * @author andy.zhou
 *
 */
public class ConsumerSenderNothingImpl extends ConsumerSenderAbs<Builder> {

    public ConsumerSenderNothingImpl(JSONObject params) {
        super(params);
    }

    @Override
    public Builder packObj(DuckulaEvent duckulaEvent, Map<String, String> datamap, Rule rule, String[] primaries) {
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
