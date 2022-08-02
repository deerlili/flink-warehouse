package com.deerlili.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.deerlili.gmall.realtime.app.function.DimAsyncFunction;
import com.deerlili.gmall.realtime.bean.OrderDetail;
import com.deerlili.gmall.realtime.bean.OrderInfo;
import com.deerlili.gmall.realtime.bean.OrderWide;
import com.deerlili.gmall.realtime.utils.DateFormatUtil;
import com.deerlili.gmall.realtime.utils.KafkaUtil;
import org.apache.commons.lang.time.DateUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * OrderWideApp 订单宽表 two-stream join
 *
 * @author lixx
 * @date 2022/7/18 10:38
 * 数据流：web/app -> nginx -> springboot -> mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> kafka(dwd)/Phoenix(dim) -> FlinkApp(redis) -> kafka(dwm)
 * 程 序：mockDb -> mysql -> FlinkCDCApp -> kafka(ZK) -> BaseDBApp -> Kafka/Phoenix(Hbase,zk,hdfs) -> OrderWideApp(Redis)
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //1.执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1  设置状态后端
        //env.setStateBackend(new FsStateBackend("HDFS://hadoop100:9000/flink/ck"));
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(10)));

        //2.读取kafka主题数据，并转换JavaBean对象&提取时间戳生成WaterMark
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        SingleOutputStreamOperator<OrderInfo> orderInfoDS = env.addSource(KafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId)).map(line -> {
            //将JSON字符串转换为JavaBean
            OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);
            //取出创建时间字段,按照空格分割
            String[] dateTimeArr = orderInfo.getCreate_time().split(" ");
            orderInfo.setCreate_date(dateTimeArr[0]);
            orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderInfo.setCreate_ts(format.parse(orderInfo.getCreate_time()).getTime());

            return orderInfo;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo orderInfo, long l) {
                        return orderInfo.getCreate_ts();
                    }
                }));

        orderInfoDS.print("orderInfoDS>>>>>>");
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = env.addSource(KafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId))
                .map(line -> {
                    OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);

                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderDetail.setCreate_ts(format.parse(orderDetail.getCreate_time()).getTime());

                    return orderDetail;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long l) {
                                return orderDetail.getCreate_ts();
                            }
                        }));

        orderDetailDS.print("orderDetailDS>>>>>>");
        //3.双流JOIN,状态编程
        SingleOutputStreamOperator<OrderWide> orderWideWithNoDimDS = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                //生产环境,为了不丢数据,设置时间 为最大网络延迟
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context context, Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        orderWideWithNoDimDS.print("orderWideWithNoDimDS>>>");

        //4.关联维度信息,这里超时时间大于60S,因为ZK的超时时间的60S

        //4.1.关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                orderWideWithNoDimDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        String birthday = dimInfo.getString("BIRTHDAY");
                        if (birthday.length() == 10) {
                            birthday = birthday + " 00:00:00";
                        }
                        System.out.println("----------"+birthday);
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        long nowTs = System.currentTimeMillis();
                        long age = (nowTs - format.parse(birthday).getTime()) / (1000 * 60 * 60 * 24 * 365L);
                        orderWide.setUser_age((int) age);
                        orderWide.setUser_gender(dimInfo.getString("GENDER"));
                    }
                },
                85,
                TimeUnit.SECONDS,
                100);

        orderWideWithUserDS.print("orderWideWithUserDS>>>");
        //4.2.关联地区维度
        //5.将数据写入kafka
        //6.启动
        env.execute("orderWideApp");
    }
}
