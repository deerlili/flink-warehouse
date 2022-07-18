package com.deerlili.gmall.realtime.app;

import com.deerlili.gmall.realtime.bean.Bean1;
import com.deerlili.gmall.realtime.bean.Bean2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.Tuple2;

/**
 * FlinkDataStreamJoinTest 双流JOIN测试
 *
 * @author lixx
 * @date 2022/7/18 9:53
 */
public class FlinkDataStreamJoinTest {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取两个端口数据创建流并读取时间戳生成WaterMark
        SingleOutputStreamOperator<Bean1> stream1 = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean1(fields[0], fields[1], Long.parseLong(fields[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean1>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Bean1>() {
                            @Override
                            public long extractTimestamp(Bean1 bean1, long l) {
                                return bean1.getTs() * 1000L;
                            }
                        }));

        SingleOutputStreamOperator<Bean2> stream2 = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean2(fields[0], fields[1], Long.parseLong(fields[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean2>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Bean2>() {
                            @Override
                            public long extractTimestamp(Bean2 bean2, long l) {
                                return bean2.getTs() * 1000L;
                            }
                        }));
        //3.双流JOIN

        SingleOutputStreamOperator<Tuple2<Bean1, Bean2>> joinDS = stream1.keyBy(Bean1::getId)
                .intervalJoin(stream2.keyBy(Bean2::getId))
                .between(Time.seconds(-5), Time.seconds(5))
                //.lowerBoundExclusive() 左开右闭
                //.upperBoundExclusive() 左闭右开 都加是开区间
                .process(new ProcessJoinFunction<Bean1, Bean2, Tuple2<Bean1, Bean2>>() {
                    @Override
                    public void processElement(Bean1 bean1, Bean2 bean2, ProcessJoinFunction<Bean1, Bean2, Tuple2<Bean1, Bean2>>.Context context, Collector<Tuple2<Bean1, Bean2>> collector) throws Exception {
                        collector.collect(new Tuple2<>(bean1, bean2));
                    }
                });
        //4.打印
        joinDS.print();
        //5.启动
        env.execute("JoinTest");
        /*
        * 测试：nc -lk 8888
        *   1001,zhang,1
        * 测试：nc -lk 9999
        *   1001,male,1
        *
        *   1001,female,2
        *   1002,li,10
        *   1001,male,3
        *   1002,female,10
        *   1001,male,3 不会打印，WaterMark是取上游最小的WaterMark,WaterMark的传递问题
        *   1001,male,11
        *
        * */
    }
}
