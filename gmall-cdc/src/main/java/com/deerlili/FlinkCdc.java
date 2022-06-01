//package com.deerlili;
//
//
//
//import com.ververica.cdc.connectors.mysql.source.MySqlSource;
//import com.ververica.cdc.connectors.mysql.table.StartupOptions;
//import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.Properties;
//
///**
// * @author deerlili
// * @date 2022/6/1
// * @des
// */
//public class FlinkCdc {
//    public static void main(String[] args) throws Exception {
//
//        Properties debeziumProperties = new Properties();
//        debeziumProperties.put("snapshot.locking.mode", "none");
//
//        // 1.通过FlinkCDC构建SourceFunction并读取数据
//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname("hadoop100")
//                .port(3306)
//                .username("test")
//                .password("123456")
//                //.scanNewlyAddedTableEnabled(true)
//                .databaseList("gmall_flink")
//                //不指定，默认所有表;指定参数，指定方式为db.table
//                .tableList("gmall_flink.base_trademark")
//                //.deserializer(new JsonDebeziumDeserializationSchema()
//                /*
//                * initial:初始化全量读取，然后binlog最新位置增量，就是先查历史数据，增量数据binlog
//                * earliest:不做初始化，从binlog开始读取。需要（先开启binlog,然后建库,建表）不然会报错
//                * latest:只会读取连接后的binlog
//                * timestamp:读取时间戳之后的数据，大于等于
//                * specificOffset:指定位置
//                * */
//                .startupOptions(StartupOptions.initial())
//                .debeziumProperties(debeziumProperties)
//                .build();
//
//        // 2.获取执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // enable checkpoint
//        env.enableCheckpointing(3000);
//
//        // 3.打印数据
//        env.fromSource(mySqlSource.).setParallelism(1)
//                .print();
//
//        // 4.启动任务
//        // env.execute("FlinkCdc");
//        env.execute();
//    }
//}