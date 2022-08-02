package com.deerlili.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.deerlili.gmall.realtime.bean.PaymentInfo;
import com.deerlili.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * PaymentWideApp 支付宽表程序
 *
 * @author lixx
 * @date 2022/8/2 17:59
 */
public class PaymentWideApp {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1  设置状态后端
        //env.setStateBackend(new FsStateBackend("HDFS://hadoop100:9000/flink/ck"));
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));

        //2.读取kafka主题创建数据流
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        DataStreamSource<String> paymentInfoDS = env.addSource(KafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId));
        DataStreamSource<String> orderWideDS = env.addSource(KafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId));

        //2.1转换为JavaBean提取时间戳生成WaterMark
        paymentInfoDS.map(line ->{
            return JSON.parseObject(line, PaymentInfo.class);
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo paymentInfo, long l) {
                        return 0;
                    }
                });
        /**
         *
         *
         * 3.双流JOIN
         * 4.将数据写入Kafka
         * 5.启动任务
         */

    }
}
