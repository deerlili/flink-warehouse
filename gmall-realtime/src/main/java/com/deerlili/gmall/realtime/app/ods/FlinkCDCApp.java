package com.deerlili.gmall.realtime.app.ods;

import com.deerlili.gmall.realtime.utils.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lixx
 * @date 2022/6/8
 */
public class FlinkCDCApp {
    public static void main(String[] args) throws Exception {

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop100")
                .port(3306)
                .username("test")
                .password("123456")
                .databaseList("gmall_flink")
                .scanNewlyAddedTableEnabled(true)
                .tableList("gmall_flink.*")
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        //// 开启 checkpoint 并指定状态后端为 hdfs 也可以：rocksdb
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop100:9000/gmall-flink/ck"));
        //// 设置hdfs用户
        //System.setProperty("HADOOP_USER_NAME", "root");
        //// 生产环境可以设置成5分钟或10分钟做一次check
        //env.enableCheckpointing(5000);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //// 设置超时时间
        //env.getCheckpointConfig().setCheckpointTimeout(10000);
        //// 允许同时存在多少个
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //// 设置最小的间隔时间
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        //// 设置任务关闭的时候保留最后一次 CK 数据
        //env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<String> streamSource =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySql Source");

        streamSource.print();

        // 数据写入Kafka
       streamSource.addSink(KafkaUtil.getKafkaProducer("ods_base_db"));

        env.execute("MySQL Binlog + Kafka");
    }
}
