package com.deerlili.gmall.realtime.app.dws;

import com.deerlili.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * KeyWordsStatsApp 搜索关键字计算
 *
 * @author lixx
 * @date 2022/8/17 17:37
 */
public class KeyWordsStatsApp {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.1.设置ck和状态后端
        //env.setStateBackend(new FsStateBackend("HDFS://hadoop100:9000/flink/ck/visitor_stats_app"));
        //System.setProperty("HADOOP_USER_NAME","root");
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));

        //使用DDL方式读取kafka数据创建表
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic ="dwd_page_log";

        tableEnv.executeSql("CREATE TABLE page_view (" +
                "common MAP<STRING,STRING>, " +
                "page MAP<STRING,STRING>," +
                "ts BIGINT, " +
                "rowTime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
                "WATERMARK FOR rowTime AS rowTime - INTERVAL '2' SECOND) " +
                "WITH ("+ KafkaUtil.getKafkaDDL(pageViewSourceTopic,groupId)+")");

        //过滤数据(上一跳为"search"和搜索词不为null)
        Table sqlQuery = tableEnv.sqlQuery(
                      "select " +
                        "   page['item'] fullword," +
                        "   rowTime" +
                        "from " +
                        "   page_view " +
                        "where page['last_page_id']='search' and page['item'] IS NOT NULL ");

        //注册UDTF,进行分词

        //分组开窗聚合
        //动态表装换为流
        //打印和写入ClickHouse
        //启动任务
    }
}
