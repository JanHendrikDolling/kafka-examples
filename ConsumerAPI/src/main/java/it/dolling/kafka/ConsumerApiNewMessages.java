package it.dolling.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerApiNewMessages {
    public static void main(String[] args) {
        Properties props = properties();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("my-topic"));

        //noinspection InfiniteLoopStatement
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }

    @NotNull
    private static Properties properties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "DESKTOP-85STEL0.localdomain:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
