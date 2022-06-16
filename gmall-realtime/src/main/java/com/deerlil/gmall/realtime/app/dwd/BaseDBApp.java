package com.deerlil.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.deerlil.gmall.realtime.app.function.DimSink;
import com.deerlil.gmall.realtime.app.function.TableProcessFunction;
import com.deerlil.gmall.realtime.bean.TableProcess;
import com.deerlil.gmall.realtime.utils.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author lixx
 * @date 2022/6/11
 * @notes flink cdc dwd db
 */
public class BaseDBApp {

    public static void main(String[] args) throws Exception {

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.1.设置CK和状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop100:9000/gmall-flink/dwd_log/ck"));
        //System.setProperty("HADOOP_USER_NAME","root");

        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        //env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 2.消费Kafka(ods_base_db)主题，主题数据创建流
        String topic = "ods_base_db";
        String groupId = "base_db_app";
        DataStreamSource<String> kafkaDs = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));
        // 2.1.脏数据处理,并每行数据转换为Json对象
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {};
        SingleOutputStreamOperator<JSONObject> jsonDs = kafkaDs.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    // 发生异常，把数据写入侧输出流
                    context.output(dirtyTag, value);
                }
            }
        });
        // 2.2.脏数据打印
        jsonDs.getSideOutput(dirtyTag).print("dirty>>>");
        // 2.1和2.2 可以替换 kafkaDS.map(JSON::parseObject);
        // 3.过滤(delete)数据-主流
        SingleOutputStreamOperator<JSONObject> filterDS = jsonDs.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                // 获取op(操作类型) c r u d
                String op = jsonObject.getString("op");
                return !"d".equals(op);
            }
        });
        // 4.flinkCDC消费配置表并处理成-广播流(另外一个MySQL)
        MySqlSource<String> mySqlConfigBuilder = new MySqlSourceBuilder<String>()
                .hostname("hadoop100")
                .port(3306)
                .username("test")
                .password("123456")
                .databaseList("realtime")
                .tableList("realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> mysqlConfigDs = env.fromSource(mySqlConfigBuilder, WatermarkStrategy.noWatermarks(), "mysql config");

        MapStateDescriptor<String, TableProcess> mapStateDescriptor =
                new MapStateDescriptor<>("mysql-state",String.class,TableProcess.class);
        BroadcastStream<String> broadcast = mysqlConfigDs.broadcast(mapStateDescriptor);

        // 5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String>  connectedStream = filterDS.connect(broadcast);

        // 6.分流处理数据-广播流数据、主流数据(根据广播流数据进行处理)
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {};
        SingleOutputStreamOperator<JSONObject> kafka =
                connectedStream.process(new TableProcessFunction(hbaseTag,mapStateDescriptor));

        // 7.提取Kafka流数据和Hbase流数据
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);

        // 8.将Kafka数据写入Kafka主题，将Hbase数据写入Phoenix表
        hbase.print("hbase>>>");
        kafka.print("kafka>>>");
        // zk hdfs hbase kafka

        // 9.启动任务
        env.execute("dwd_db_base_app");
    }

}
