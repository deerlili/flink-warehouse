package com.deerlil.gmall.realtime.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lixx
 * @date 2022/6/11
 * @notes mysql table process configure
 */
public class MySqlUtil {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mysqlSource = new MySqlSourceBuilder<String>()
                .hostname("hadoop100")
                .port(3306)
                .username("test")
                .password("123456")
                .databaseList("realtime")
                .tableList("realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> mysqlDs = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql source");

        mysqlDs.print();

        env.execute();

    }

}
