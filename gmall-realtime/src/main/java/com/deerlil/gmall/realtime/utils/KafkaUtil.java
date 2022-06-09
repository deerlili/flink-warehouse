package com.deerlil.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @author lixx
 * @date 2022/6/8
 * @notes Kafka Util
 */
public class KafkaUtil {
    private static String BROKER_LIST = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(BROKER_LIST, topic, new SimpleStringSchema());
    }
}
