package com.deerlili.gmall.realtime.app.ods;

import com.deerlili.gmall.realtime.utils.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author lixx
 * @date 2022/6/8
 */
public class FlinkCDCApp {
    public static void main(String[] args) throws Exception {

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("decimal.handling.mode", "string");
        /**
         * precise 以java中的精确类型来表示值
         * double  使用比较容易，但是会造成精度损失
         * string  也比较容易使用，但是会造成字段语意信息丢失
         */

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop100")
                .port(3306)
                .username("test")
                .password("123456")
                .databaseList("gmall_flink")
                .scanNewlyAddedTableEnabled(true)
                .tableList("gmall_flink.order_info,gmall_flink.order_detail")
                .startupOptions(StartupOptions.latest())
                /*
                 * initial:初始化全量读取，然后binlog最新位置增量，就是先查历史数据，增量数据binlog
                 * earliest:不做初始化，从binlog开始读取。需要（先开启binlog,然后建库,建表）不然会报错
                 * latest:只会读取连接后的binlog
                 * timestamp:读取时间戳之后的数据，大于等于
                 * specificOffset:指定位置
                 * */
                .debeziumProperties(properties)
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
