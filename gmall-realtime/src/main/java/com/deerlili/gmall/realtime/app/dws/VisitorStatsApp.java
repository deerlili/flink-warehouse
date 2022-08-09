package com.deerlili.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.deerlili.gmall.realtime.bean.VisitorStats;
import com.deerlili.gmall.realtime.utils.ClickHouseUtil;
import com.deerlili.gmall.realtime.utils.DateFormatUtil;
import com.deerlili.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * VisitorStatsApp 访客主题宽表
 *
 * @author lixx
 * @date 2022/8/3 14:17
 * 要不要把多个明细的同样的维度统计在一起?
 * 因为单位时间内 mid 的操作数据非常有限不能明显的压缩数据量（如果是数据量够大， 或者单位时间够长可以）
 * 所以用常用统计的四个维度进行聚合 渠道、新老用户、app 版本、省市区域
 * 度量值包括 启动、日活（当日首次启动）、访问页面数、新增用户数、跳出数、平均页面停留时长、总访问时长
 * 聚合窗口： 10 秒
 *
 * 各个数据在维度聚合前不具备关联性，所以先进行维度聚合
 * 进行关联	这是一个 fulljoin
 * 可以考虑使用 flinksql  完成
 *
 * 数据流：web/app -> Nginx -> SpringBoot -> Kafka（ods）-> FlinkApp -> Kafka（dwd) -> FlinkApp -> Kafka（dwm)
 *          -> FlinkApp -> ClickHouse
 * 程 序：mockLog -> Nginx -> Logger.sh -> Kafka（zk）-> BaseLogApp -> Kafka  -> uv/uj
 *          -> Kafka -> VisitorStatsApp -> ClickHouse
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);

        //设置检查点
        //env.setStateBackend(new FsStateBackend("HDFS://hadoop100:9000/flink/ck/visitor_stats_app"));
        //System.setProperty("HADOOP_USER_NAME","root");
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));

        //读取kafka数据创建流,pv、uv、跳转明细主题中获取数据
        String groupId = "visitor_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        DataStreamSource<String> uvDs = env.addSource(KafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDs = env.addSource(KafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId));
        DataStreamSource<String> pvDs = env.addSource(KafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));

        //将每个流处理成相同的数据类型
        /**
         * {
         * 	"common": {
         * 		"ar": "500000",
         * 		"ba": "Xiaomi",
         * 		"ch": "oppo",
         * 		"is_new": "1",
         * 		"md": "Xiaomi Mix2 ",
         * 		"mid": "mid_168428",
         * 		"os": "Android 11.0",
         * 		"uid": "3080",
         * 		"vc": "v2.1.111"
         *        },
         * 	"page": {
         * 		"during_time": 1113,
         * 		"item": "1,6",
         * 		"item_type": "sku_ids",
         * 		"last_page_id": "orders_unpaid",
         * 		"page_id": "trade"
         *    },
         * 	"ts": 1657854502000
         * }
         */

        //3.1.处理UV数据

        SingleOutputStreamOperator<VisitorStats> visitorStatsUvDs = uvDs.map(line -> {
            JSONObject object = JSON.parseObject(line);
            //提起公共字段
            JSONObject common = object.getJSONObject("common");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    object.getLong("ts"));
        });

        //3.2.处理UJ数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsUjDs = ujDs.map(line -> {
            JSONObject object = JSON.parseObject(line);
            //提起公共字段
            JSONObject common = object.getJSONObject("common");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    object.getLong("ts"));
        });

        //3.3.处理PV数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsPvDs = pvDs.map(line -> {
            JSONObject object = JSON.parseObject(line);
            //提起公共字段
            JSONObject common = object.getJSONObject("common");
            //获取页面信息
            JSONObject page = object.getJSONObject("page");
            String lastPageId = page.getString("last_page_id");
            long sv = 0L;
            if (lastPageId == null || lastPageId.length() <= 0) {
                sv = 1L;
            }
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 1L, sv, 0L, page.getLong("during_time"),
                    object.getLong("ts"));
        });

        //4.UNION几个流
        DataStream<VisitorStats> unionDs = visitorStatsUvDs.union(visitorStatsUjDs,visitorStatsPvDs);
        //5.提取时间戳生成WaterMark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWaterMarkDs = unionDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(12))
                        .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats visitorStats, long l) {
                                return visitorStats.getTs();
                            }
                        }));
        //6.按照维度信息分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWaterMarkDs.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                return new Tuple4<String, String, String, String>(
                        visitorStats.getAr(),
                        visitorStats.getCh(),
                        visitorStats.getIs_new(),
                        visitorStats.getVc());
            }
        });
        //7.开窗聚合 10S的滚动窗口,增量聚合和全量聚合一起使用 reduce(ReduceFunction,WindowFunction)
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        SingleOutputStreamOperator<VisitorStats> result = windowedStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats t1, VisitorStats t2) throws Exception {
                //t1.setPv_ct(t1.getPv_ct()+t2.getPv_ct());
                //滚动窗口可以用上下两种，滑动窗口只能用下面这种（new）
                return new VisitorStats(t1.getStt(),
                        t1.getEdt(),
                        t1.getVc(),
                        t1.getCh(),
                        t1.getAr(),
                        t1.getIs_new(),
                        t1.getUv_ct() + t2.getUv_ct(),
                        t1.getPv_ct() + t2.getPv_ct(),
                        t1.getSv_ct() + t2.getSv_ct(),
                        t1.getUj_ct() + t2.getUj_ct(),
                        t1.getDur_sum() + t2.getDur_sum(),
                        t1.getTs());
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<VisitorStats> iterable, Collector<VisitorStats> collector) throws Exception {
                long start = timeWindow.getStart();
                long end = timeWindow.getEnd();
                VisitorStats visitorStats = iterable.iterator().next();
                visitorStats.setStt(DateFormatUtil.toYmdHms(start));
                visitorStats.setEdt(DateFormatUtil.toYmdHms(end));
                collector.collect(visitorStats);
            }
        });

        result.print("result>>>>");

        //将数据写ClickHouse
        //JavaBean和表字段顺序一样可以不写字段名
        result.addSink(ClickHouseUtil.getClickHouseSink("insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"));
        //启动
        env.execute("VisitorStatsApp");

    }
}
