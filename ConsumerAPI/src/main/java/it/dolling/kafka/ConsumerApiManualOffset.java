package it.dolling.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ConsumerApiManualOffset {

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties());

        String topic = "my-topic";
        consumer.subscribe(Collections.singletonList(topic));
        consumer.subscribe(Collections.singletonList(topic), new PartitionOffsetAssignerListener(consumer));

        final int minBatchSize = 150;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

        //noinspection InfiniteLoopStatement
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                insertIntoDb(buffer);
                consumer.commitSync();
                buffer.clear();
            }
        }
    }

    private static void insertIntoDb(List<ConsumerRecord<String, String>> records) {
        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }

        records.clear();
    }

    @NotNull
    private static Properties properties() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "DESKTOP-85STEL0.localdomain:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

}
