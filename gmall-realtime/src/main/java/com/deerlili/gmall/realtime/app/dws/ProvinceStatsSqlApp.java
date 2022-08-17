package com.deerlili.gmall.realtime.app.dws;

import com.deerlili.gmall.realtime.bean.ProvinceStats;
import com.deerlili.gmall.realtime.utils.ClickHouseUtil;
import com.deerlili.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ProvinceStatsSqlApp FlinkSQL 实现地区主题宽表计算
 *
 * @author lixx
 * @date 2022/8/11 12:55
 * 数据流：web/app -> nginx -> springboot -> mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> kafka(dwd)/Phoenix(dim) -> FlinkApp(redis) -> kafka(dwm) - FlinkApp - Clickhouse
 * 程 序：mockDb -> mysql -> FlinkCDCApp -> kafka(ZK) -> BaseDBApp -> Kafka/Phoenix(Hbase,zk,hdfs) -> OrderWideApp(Redis) -> kafka -> ProvinceStatsSqlApp -> Clickhouse
 */
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 1.1.设置ck和状态后端
        //env.setStateBackend(new FsStateBackend("HDFS://hadoop100:9000/flink/ck/visitor_stats_app"));
        //System.setProperty("HADOOP_USER_NAME","root");
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
        
        // TODO 2.使用DDL创建表，提取时间戳生成watermark
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";

        tableEnv.executeSql("create table order_wide(" +
                "province_id BIGINT," +
                "province_name STRING," +
                "province_area_code STRING," +
                "province_iso_code STRING," +
                "province_3166_2_code STRING," +
                "order_id BIGINT," +
                "split_total_amount DECIMAL," +
                "create_time STRING," +
                "rowTime as TO_TIMESTAMP(create_time)," +
                "WATERMARK FOR rowTime as rowTime - INTERVAL '1' SECOND" +
                ") WITH (" + KafkaUtil.getKafkaDDL(orderWideTopic,groupId) +
                ")");
        //TODO 3.查询数据 分组，开窗，集合
        Table sqlQueryTable = tableEnv.sqlQuery("select " +
                "    DATE_FORMAT(TUMBLE_START(rowTime,INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss' stt," +
                "    DATE_FORMAT(TUMBLE_END(rowTime,INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss' edt," +
                "    province_id,province_name,province_area_code,province_iso_code,province_3166_2_code," +
                "    COUNT( DISTINCT order_id) order_count," +
                "    sum(total_amount) order_amount" +
                "from order_wide " +
                "group by " +
                "    province_id,province_name,province_area_code,province_iso_code,province_3166_2_code," +
                "    TUMBLE(rowTime, INTERVAL '10' SECOND )");
        //TODO 4.动态表转换为流
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(sqlQueryTable, ProvinceStats.class);
        //TODO 5.打印并写入ClickHouse
        /**
         * create table province_stats (
         *  stt DateTime,
         *  edt DateTime,
         *  province_id	UInt64,
         *  province_name String,
         *  area_code String,
         *  iso_code String,
         *  iso_3166_2 String,
         *  order_amount Decimal64(2),
         *  order_count UInt64,
         *  ts UInt64
         * )engine =ReplacingMergeTree(ts)
         *  partition by toYYYYMMDD(stt)
         *  order by	(stt,edt,province_id);
         */

        provinceStatsDataStream.addSink(ClickHouseUtil.<ProvinceStats>getClickHouseSink("insert into province_stats values(?,?,?,?,?,?,?,?,?,?)"));
        //TODO 6.启动

        env.execute("");



    }
}
