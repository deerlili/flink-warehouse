package com.deerlili.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.deerlili.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * UserJumpDetailApp 用户跳出统计
 *
 * @author lixx
 * @date 2022/7/8 15:41
 */
public class UserJumpDetailApp {

    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、设置后端状态
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        // 3、读取Kafka dwd_page_log主题数据
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwd_user_jump_detail";
        FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaConsumer(sourceTopic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        SingleOutputStreamOperator<JSON> jsonDS = kafkaDS.process(new ProcessFunction<String, JSON>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSON>.Context context, Collector<JSON> collector) throws Exception {
                try {
                    JSONObject parseObject = JSON.parseObject(s);
                    collector.collect(parseObject);
                } catch (Exception e) {
                    context.output(new OutputTag<String>("dirty") {
                    }, s);
                }
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());

        jsonDS.print(">>>>>>");


        env.execute("UserJumpDetailApp");
    }
}
