package com.example.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class MessageProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String topic, String message) {
        var completableFuture = kafkaTemplate.send(topic, message);
        completableFuture.whenComplete(((sendResult, exception) -> {
            if(exception != null) {
                System.out.println(exception.getMessage());
            }else{
                System.out.println(sendResult);
            }
        }));
    }
}
