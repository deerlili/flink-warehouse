package com.deerlili.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * KafkaUtil kafka producer and consumer method
 *
 * @author lixx
 * @date 2022/6/8
 */
public class KafkaUtil {

    private static final String BROKER_LIST = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String DEFAULT_TOPIC = "dwd_default_topic";

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<>(BROKER_LIST, topic, new SimpleStringSchema());
    }

    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);

        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC
                , kafkaSerializationSchema
                , properties
                , FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        // 开启checkpoint,ck不开FlinkKafkaProducer.Semantic.EXACTLY_ONCE不生效
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
    }
}
