package com.deerlil.gmall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @author lixx
 * @date 2022/6/8
 * @notes Customer Debezium Deserialization Schema
 */
public class CustomerDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        JSONObject result = new JSONObject();


    }

    @Override
    public TypeInformation<String> getProducedType() {
        return null;
    }
}
