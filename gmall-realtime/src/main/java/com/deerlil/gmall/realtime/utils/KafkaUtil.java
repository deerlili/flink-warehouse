package com.deerlil.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @author lixx
 * @date 2022/6/8
 * @notes Kafka Util
 */
public class KafkaUtil {
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        final String brokerList = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
        return new FlinkKafkaProducer<String>(brokerList, topic, new SimpleStringSchema());
    }
}
