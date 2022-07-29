package com.deerlili.gmall.realtime.app.dwm;

import java.text.SimpleDateFormat;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSON;
import com.deerlili.gmall.realtime.bean.OrderDetail;
import com.deerlili.gmall.realtime.bean.OrderInfo;
import com.deerlili.gmall.realtime.bean.OrderWide;
import com.deerlili.gmall.realtime.utils.KafkaUtil;

/**
 * OrderWideApp 订单宽表 two-stream join
 *
 * @author lixx
 * @date 2022/7/18 10:38
 */
public class OrderWideNoDimApp {
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
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(10)));

        //2.读取kafka主题数据，并转换JavaBean对象&提取时间戳生成WaterMark
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        DataStreamSource<String> stringDataStreamSource = env.addSource(KafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId));

        SingleOutputStreamOperator<OrderInfo> orderInfoDS = stringDataStreamSource.map(line -> {
            OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);

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


        //3.双流JOIN
        SingleOutputStreamOperator<OrderWide> orderWideWithNoDimDS = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                //生产环境给最大延迟时间
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context context, Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        orderWideWithNoDimDS.print("orderWideWithNoDimDS>>>");

        /*
        * 测试：启动zk,kafka,hdfs,hbase,mock_db
        * 程序：ods(FlinkCDCApp),BaseDBApp,OrderWideApp
        * */

        //4.关联维度信息

        //5.将数据写入kafka

        //6.启动
        env.execute("orderWideApp");

    }

}
