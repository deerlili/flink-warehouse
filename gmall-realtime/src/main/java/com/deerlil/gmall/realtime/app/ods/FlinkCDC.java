package com.deerlil.gmall.realtime.app.ods;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

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
                .scanNewlyAddedTableEnabled(true)
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启 checkpoint 并指定状态后端为 hdfs 也可以：rocksdb
        env.setStateBackend(new FsStateBackend("hdfs://hadoop100:9000/gmall-flink/ck"));
        // 设置hdfs用户
        System.setProperty("HADOOP_USER_NAME", "root");

        // 生产环境可以设置成5分钟或10分钟做一次check
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // 允许同时存在多少个
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // 设置最小的间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        // 设置任务关闭的时候保留最后一次 CK 数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        // 数据写入Kafka
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySql Source")
                // set 4 parallel source tasks
                .setParallelism(4)
                .print()
                // use parallelism 1 for sink to keep message ordering
                .setParallelism(1);

        new FlinkKafkaProducer<String>()

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
