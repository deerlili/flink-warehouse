package com.deerlil.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.deerlil.gmall.realtime.utils.KafkaUtil;
import com.mysql.cj.util.StringUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

/**
 * UniqueVisitApp 独立访客
 *
 * @author lixx
 * @date 2022/6/24 17:00
 */
public class UniqueVisitApp {

    /**
     * 1、获取执行环境
     * 2、设置Checkpoint
     * 3、读取Kafka dwd_page_log主题数据
     * 4、将每行数据转换为JSON对象
     * 5、过来数据-状态编程-只保留每个mid每天第一次登陆的数据
     * 6、将数据写入Kafka
     * 7、启动任务
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStateBackend(new FsStateBackend("HDFS://hadoop100:9000/flink/ck"));
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);

        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwd_unique_visit";
        DataStreamSource<String> KafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        SingleOutputStreamOperator<JSONObject> jsonObjDS = KafkaDS.map(JSON::parseObject);

        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> dateState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("date-state", String.class);
                dateState = getRuntimeContext().getState(valueStateDescriptor);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                // 获取上一条页面信息
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                // 判断上一条页面是否为Null
                if (lastPageId == null || lastPageId.length() <= 0) {
                    // 取出状态数据
                    String lastDate = dateState.value();
                    // 取出今天的日期
                    String curDate = simpleDateFormat.format(jsonObject.getLong("ts"));
                    // 判断两个日期是否相同
                    if (!curDate.equals(lastDate)) {
                        dateState.update(curDate);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        });

        uvDS.print();
        uvDS.map(jsonObject -> jsonObject.toJSONString()).addSink(KafkaUtil.getKafkaProducer(sinkTopic));

        env.execute("UniqueVisitApp");
    }
}
