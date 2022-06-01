package com.deerlili;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.MySqlSource;

import java.util.Properties;

/**
 * @author deerlili
 * @date 2022/6/1
 * @des cdc version 2.0.2
 */

public class MySqlBinlogSourceExample {
    public static void main(String[] args) throws Exception {
        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.locking.mode", "none");
        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("hadoop100")
                .port(3306)
                .databaseList("gmall_flink")
                .tableList("gmall_flink.base_trademark")
                .username("test")
                .password("123456")
                .deserializer(new StringDebeziumDeserializationSchema())
                .debeziumProperties(debeziumProperties)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(sourceFunction)
                .print().setParallelism(1);
        env.execute();
    }
}
