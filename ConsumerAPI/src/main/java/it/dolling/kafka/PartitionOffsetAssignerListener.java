package it.dolling.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class PartitionOffsetAssignerListener implements ConsumerRebalanceListener {

    private final KafkaConsumer consumer;

    public PartitionOffsetAssignerListener(KafkaConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        consumer.seekToBeginning(partitions);
    }
}
