/*
package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class AckMessageListener implements AcknowledgingMessageListener<String, String> {
    @Override
    @KafkaListener(topics = "my-topic")
    public void onMessage(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) {
        //log.info("Consumer Record: {}", consumerRecord);
        assert acknowledgment != null;
       // The default AckMode is BATCH.
        acknowledgment.acknowledge();
    }
}
*/
