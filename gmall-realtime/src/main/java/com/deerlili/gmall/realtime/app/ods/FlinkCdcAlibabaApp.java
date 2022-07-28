package com.deerlili.gmall.realtime.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.deerlili.gmall.realtime.app.function.CustomerDeserializationSchemaAlibaba;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lixx
 * @date 2022/6/8
 */
public class FlinkCdcAlibabaApp {
    public static void main(String[] args) throws Exception {

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DebeziumSourceFunction<String> build = MySQLSource.<String>builder()
                .hostname("hadoop100")
                .port(3306)
                .username("test")
                .password("123456")
                .databaseList("gmall_flink")
                .tableList("gmall_flink.*")
                .startupOptions(StartupOptions.latest())
                .deserializer(new CustomerDeserializationSchemaAlibaba())
                .build();


        DataStreamSource<String> DS = env.addSource(build, "tt");

        DS.print();

        env.execute("MySQL Binlog + Kafka");
    }
}
