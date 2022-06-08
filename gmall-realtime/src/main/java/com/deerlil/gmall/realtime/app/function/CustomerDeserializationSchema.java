package com.deerlil.gmall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @author lixx
 * @date 2022/6/8
 * @notes Customer Debezium Deserialization Schema
 */
public class CustomerDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        JSONObject result = new JSONObject();

        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];

        Struct value = (Struct) sourceRecord.value();

        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            Schema beforeSchema = before.schema();
            List<Field> beforeFields = beforeSchema.fields();
            for (int i = 0; i < beforeFields.size(); i++) {
                Field field = beforeFields.get(i);
                Object beforeValue = before.get(field);
                beforeJson.put(field.name(),beforeValue);
            }
        }

        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            Schema afterSchema = after.schema();
            List<Field> afterJsonFields = afterSchema.fields();
            for (int i = 0; i < afterJsonFields.size(); i++) {
                Field field = afterJsonFields.get(i);
                Object afterValue = after.get(field);
                afterJson.put(field.name(),afterValue);
            }
        }

        // 操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

        result.put("database",database);
        result.put("tableName",tableName);
        result.put("before",beforeJson);
        result.put("after",afterJson);
        result.put("type",operation);

        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
