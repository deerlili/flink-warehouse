package com.deerlil.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * KafkaUtil kafka producer and consumer method
 *
 * @author lixx
 * @date 2022/6/8
 */
public class KafkaUtil {

    private static final String BROKER_LIST = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<>(BROKER_LIST, topic, new SimpleStringSchema());
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
    }
}
