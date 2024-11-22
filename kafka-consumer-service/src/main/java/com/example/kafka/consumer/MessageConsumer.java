package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class MessageConsumer {

    @KafkaListener(topics={"my-topic"}, groupId = "my-group-id")
    public void listen(String message) {
        System.out.println(message);
    }

    @KafkaListener(topics ={"my-topic"}, groupId = "my-group-id-1")
    public void onMessage(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.PARTITION) int partition) {
        System.out.println(message + "===" + partition + "===" + topic);
    }

    @KafkaListener(
            topicPartitions = @TopicPartition(topic = "my-topic",
                    partitionOffsets = {
                            @PartitionOffset(partition = "0", initialOffset = "0"),
                            @PartitionOffset(partition = "3", initialOffset = "0")}
            ))
    public void onMessage(@Payload String message, @Header(KafkaHeaders.PARTITION) int partition) {
        System.out.println(message + "===" + partition);
    }

    //specify only the partitions without the offset
    @KafkaListener(topicPartitions = @TopicPartition(topic = "my-topic", partitions = { "0", "1" }))
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) {
        System.out.println(consumerRecord.toString());
    }
}
