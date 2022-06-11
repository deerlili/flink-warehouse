package com.deerlil.gmall.realtime.utils;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lixx
 * @date 2022/6/11
 * @notes mysql table process configure
 */
public class MySqlUtilTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DebeziumSourceFunction<String> build = new MySqlSource.Builder()
                .hostname("hadoop100")
                .port(3306)
                .username("test")
                .password("123456")
                .databaseList("realtime")
                .tableList("realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> ds = env.addSource(build);
        ds.print();

        env.execute();

    }

}
