package com.deerlili.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.deerlili.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * UserJumpDetailApp 用户跳出明细
 *
 * @author lixx
 * @date 2022/7/8 15:41
 *
 * 数据流：web/app -> Nginx -> SpringBoot -> Kafka（ods）-> FlinkApp -> Kafka（dwd) -> FlinkApp -> kafka -> kafka(dwm)
 * 程 序：mockLog -> Nginx -> Logger.sh -> Kafka（zk）-> BaseLogApp -> Kafka -> UserJumpDetailApp -> kafka
 */
public class UserJumpDetailApp {

    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //生产环境和kafka分区数保持一致
        env.setParallelism(1);

        //2.设置后端状态
        //env.setStateBackend(new FsStateBackend("HDFS://hadoop100:9000/flink/ck"));
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);

        //3.读取Kafka dwd_page_log主题数据
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwd_user_jump_detail";
        FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaConsumer(sourceTopic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        // 4.将每行数据转换为JSON对象并提取时间戳生成Watermark
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        }));
        /**
         * 设置乱序时间
         * WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
         */

        // 5.定义模式系列(上一跳last_page_id为null,返回数据，下一跳last_page_id为null，返回数据，或者事件时间超过10S)
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).within(Time.seconds(10));

        // 5.使用循环模式定义模式系列（上面前后过滤条件完全一样可以改为循环模式）
        //Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
        //    @Override
        //    public boolean filter(JSONObject value) throws Exception {
        //        String lastPageId = value.getJSONObject("page").getString("last_page_id");
        //        return lastPageId == null || lastPageId.length() <= 0;
        //    }
        //})
        //        .times(2)
        //        .consecutive() //指定严格近邻（next）,不加相当于上面next改为followedBy
        //        .within(Time.seconds(10));

        // 6.将模式作用到流上
        PatternStream<JSONObject> patternStream =
                CEP.pattern(jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"))
                        , pattern);

        // 7.提取匹配上的和超时时间
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("timeOut") {
        };
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("start").get(0);
                // 超时了，拿出第一条数据
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0);
                // 两条数据都存在，也只要第一条数据
            }
        });
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeOutTag);

        // 8.UNION两种事件
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);

        // 9.将数据写入Kafka
        unionDS.print();
        unionDS.map(json -> json.toJSONString()).addSink(KafkaUtil.getKafkaProducer(sinkTopic));

        // 10.启动任务
        env.execute("UserJumpDetailApp");
    }
}
