package com.deerlili.gmall.realtime;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lixx
 * @date 2022/6/2
 * @notes MySql Binlog DataStream Source
 */
public class MySqlBinlogExampleTest {

    public static void main(String[] args) throws Exception {

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop100")
                .port(3306)
                .username("test")
                .password("123456")
                .databaseList("gmall_flink")
                .tableList("gmall_flink.base_trademark")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySql Source")
                // set 4 parallel source tasks
                .setParallelism(4)
                .print()
                // use parallelism 1 for sink to keep message ordering
                .setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");

    }
}
