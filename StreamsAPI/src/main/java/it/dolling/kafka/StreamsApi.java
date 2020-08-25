package it.dolling.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.jetbrains.annotations.NotNull;

import java.util.Properties;

public class StreamsApi {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream("my-topic").mapValues(valueMapper()).to("my-topic-output");

        KafkaStreams streams = new KafkaStreams(builder.build(), properties());
        streams.start();
    }

    @NotNull
    private static ValueMapper<String, String> valueMapper() {
        return value -> {
            System.out.println(value);
            return value.repeat(10);
        };
    }

    @NotNull
    private static Properties properties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "DESKTOP-85STEL0.localdomain:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }
}
