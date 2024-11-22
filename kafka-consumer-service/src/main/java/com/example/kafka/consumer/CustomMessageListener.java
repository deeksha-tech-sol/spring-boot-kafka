package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

@Component
public class CustomMessageListener implements MessageListener<String, String> {

    @Override
    public void onMessage(ConsumerRecord<String, String> record) {

        String message = record.value();
        // Process the received message
        System.out.println(message);
    }
}
