package com.deerlili;

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
class MySqlBinlogExample {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop100")
                .port(3306)
                .username("test")
                .password("123456")
                // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .databaseList("gmall_flink")
                // 不指定，默认所有表;指定参数，指定方式为db.table
                .tableList("gmall_flink.base_trademark,gmall_flink.activity_info")
                .scanNewlyAddedTableEnabled(true)
                /*
                 * initial:初始化全量读取，然后binlog最新位置增量，就是先查历史数据，增量数据binlog
                 * earliest:不做初始化，从binlog开始读取。需要（先开启binlog,然后建库,建表）不然会报错
                 * latest:只会读取连接后的binlog
                 * timestamp:读取时间戳之后的数据，大于等于
                 * specificOffset:指定位置
                 * */
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "Mysql Source")
                // set 4 parallel source tasks
                .setParallelism(4)
                .print()
                // use parallelism 1 for sink to keep message ordering
                .setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");

    }
}
