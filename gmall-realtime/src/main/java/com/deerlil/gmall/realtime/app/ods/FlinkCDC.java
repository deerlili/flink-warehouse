package com.deerlil.gmall.realtime.app.ods;

import com.deerlil.gmall.realtime.utils.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lixx
 * @date 2022/6/8
 * @notes ods flink cdc
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop100")
                .port(3306)
                .username("test")
                .password("123456")
                .databaseList("gmall_flink")
                // .scanNewlyAddedTableEnabled(true)
                .tableList("gmall_flink.base_trademark")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySql Source").setParallelism(1);

        streamSource.print();
        // 数据写入Kafka
       streamSource.addSink(KafkaUtil.getKafkaProducer("ods_base_db"));

        env.execute("MySQL Binlog + Kafka");
    }
}
