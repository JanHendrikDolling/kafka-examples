package it.dolling.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;

import java.util.Properties;

public class ProducerApi {

    public static void main(String[] args) {
        final Producer<String, String> producer = new KafkaProducer<>(properties());

        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
        }

        producer.close();
    }

    @NotNull
    private static Properties properties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "DESKTOP-85STEL0.localdomain:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }
}
