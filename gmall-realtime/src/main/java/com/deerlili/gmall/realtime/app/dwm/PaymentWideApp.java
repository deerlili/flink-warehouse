package com.deerlili.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.deerlili.gmall.realtime.bean.OrderWide;
import com.deerlili.gmall.realtime.bean.PaymentInfo;
import com.deerlili.gmall.realtime.bean.PaymentWide;
import com.deerlili.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * PaymentWideApp 支付宽表程序
 *
 * @author lixx
 * @date 2022/8/2 17:59
 * 数据流：web/app -> nginx -> springboot -> mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> kafka(dwd)/Phoenix(dim)
 *              -> FlinkApp(redis) -> kafka(dwm) - FlinkApp -> kafka(dwm)
 * 程 序：mockDb -> mysql -> FlinkCDCApp -> kafka(ZK) -> BaseDBApp -> Kafka/Phoenix(Hbase,zk,hdfs)
 *            -> OrderWideApp(Redis) -> kafka -> PaymentWideApp -> kafka
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
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
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS1 = paymentInfoDS.map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo paymentInfo, long l) {
                                // TODO SimpleDateFormat 不能提出去，这样线程不安全，线程共享，可以使用DateFormatUtil
                                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return format.parse(paymentInfo.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return l;
                                }
                            }
                        }));
        SingleOutputStreamOperator<OrderWide> orderWideDS1 = orderWideDS
                .map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide orderWide, long l) {
                                // TODO SimpleDateFormat 不能提出去，这样线程不安全，线程共享，可以使用DateFormatUtil
                                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return format.parse(orderWide.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return l;
                                }
                            }
                        }));

        //3.双流JOIN
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS1
                .keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS1.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(10))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context context, Collector<PaymentWide> collector) throws Exception {
                        collector.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });
        paymentWideDS.print("paymentWideDS>>>");
        //4.将数据写入Kafka
        paymentWideDS.map(JSONObject::toJSONString).addSink(KafkaUtil.getKafkaProducer(paymentWideSinkTopic));
        //5.启动任务
        env.execute("PaymentWideApp");

    }
}
