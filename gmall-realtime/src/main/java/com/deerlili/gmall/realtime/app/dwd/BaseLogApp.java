package com.deerlili.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.deerlili.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author lixx
 * @date 2022/6/10
 * @notes base log app dwd
 * 数据流：web/app -> Nginx -> SpringBoot -> Kafka（ods）-> FlinkApp -> Kafka（dwd)
 * 程 序：mockLog -> Nginx -> Logger.sh -> Kafka（zk）-> BaseLogApp -> Kafka
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.1 设置CK和状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop100:9000/flink/ck"));
        //System.setProperty("HADOOP_USER_NAME","root");
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        //env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 2.消费ods_base_log主题，创建数据流
        String topic = "ods_base_log";
        String groupId = "base_log_app";
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));

        // 3.将每行数据转换为Json对象
        // 脏数据标记,如果传输的数据格式不是JSON的把数据写入侧输出流，防止异常
        OutputTag<String> outputTag = new OutputTag<String>("Dirty"){};
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    // 发生异常，把数据写入侧输出流
                    context.output(outputTag, value);
                }
            }
        });

        // 打印脏数据
        jsonObjDS.getSideOutput(outputTag).print("Dirty>>>");

        // {
        //  "common":
        //      {"ar":"440000","ba":"Xiaomi","ch":"oppo","is_new":"1","md":"Xiaomi 10 Pro ","mid":"mid_17","os":"Android 11.0","uid":"10","vc":"v2.1.134"},
        //  "start":
        //      {"entry":"notice","loading_time":14234,"open_ad_id":13,"open_ad_ms":2664,"open_ad_skip_ms":2219},
        //  "ts":1608259053000
        //}

        // 4.新老用户校验，状态编程
        // 根据is_new判断是否为新老用户，0老用户，1新用户（但是软件卸载后重新下载后是1，需要判断是否存在-状态，
        // 如果存在就把is_new改为0,不存在-状态，就把状态改为0，is_new不做变化）
        SingleOutputStreamOperator<JSONObject> jsonObjWithFlag = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<String>("value-state", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        // 获取数据中“is_new”标记
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        // 判断is_new是否为1
                        if ("1".equals(isNew)) {
                            // 获取状态数据
                            String state = valueState.value();
                            if (state != null) {
                                // 状态存在，修改is_new标记
                                jsonObject.getJSONObject("common").put("is_new", "0");
                            } else {
                                // 状态不存在，修改状态值
                                valueState.update("1");
                            }
                        }
                        return jsonObject;
                    }
                });

        // {
        //  "common":{"ar":"110000","ba":"Xiaomi","ch":"wanDouJia","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_20","os":"Android 10.0","uid":"26","vc":"v2.1.134"},
        //  "start":{"entry":"icon","loading_time":7568,"open_ad_id":10,"open_ad_ms":9519,"open_ad_skip_ms":1854},
        //  "ts":1608259054000
        //}
        // 5.分流，侧输出流（页面：主流，启动：侧输出流，曝光：侧输出流）
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};

        SingleOutputStreamOperator<String> pageDS = jsonObjWithFlag.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) {
                // 获取启动日志字段
                String start = jsonObject.getString("start");
                if (start != null && start.length() > 0) {
                    // 将数据写入启动日志侧输出流
                    context.output(startTag, jsonObject.toJSONString());
                } else {
                    // 将数据写入页面日志主流
                    collector.collect(jsonObject.toJSONString());
                    // 取出数据中的曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        // 获取页面ID
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            // 添加页面ID
                            display.put("page_id", pageId);
                            // 将数据写出到曝光侧输出流
                            context.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        // 6.提取侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        // 7.将三个流进行打印并输出到对应的Kafka主题
        startDS.print("Start>>>");
        pageDS.print("Page>>>");
        displayDS.print("Display>>>");

        startDS.addSink(KafkaUtil.getKafkaProducer("dwd_start_log"));
        pageDS.addSink(KafkaUtil.getKafkaProducer("dwd_page_log"));
        displayDS.addSink(KafkaUtil.getKafkaProducer("dwd_display_log"));

        // 8.启动任务
        env.execute("BaseLogAPP");
    }
}
