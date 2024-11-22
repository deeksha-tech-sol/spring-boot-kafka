package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    KafkaProperties properties;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

    @Bean
    public DefaultErrorHandler errorHandler() {
        FixedBackOff fixedBackOff = new FixedBackOff(1000L, 2);
        var exponentialBackOff = new ExponentialBackOffWithMaxRetries(2);
        exponentialBackOff.setInitialInterval(1000L);
        exponentialBackOff.setMultiplier(2L);
        exponentialBackOff.setMaxInterval(4000L);
       // var errorHandler = new DefaultErrorHandler(exponentialBackOff);
        var errorHandler = new DefaultErrorHandler(
                consumerRecordRecoverer,
                //publishingRecoverer(),
                //fixedBackOff
                exponentialBackOff
        );
        //These will be ignored
        var exceptionsToIgnoreList = List.of(NullPointerException.class, DeserializationException.class);
        exceptionsToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);

        //Only These will be retried
        var exceptionsToRetryList = List.of(IllegalArgumentException.class);
        exceptionsToRetryList.forEach(errorHandler::addRetryableExceptions);

        errorHandler.setRetryListeners((consumerRecord, ex, deliveryAttempt) -> {
           System.out.println("Failed Record in Retry Listener, Exception: {}, deliveryAttempt: {}"+ ex.getMessage()+ deliveryAttempt);
        });

        return errorHandler;
    }

    ConsumerRecordRecoverer consumerRecordRecoverer = (record, exception) -> {
       // log.error("Exception is : {} Failed Record : {} ", exception, record);
        if (exception.getCause() instanceof RecoverableDataAccessException) {
            System.out.println("Inside the recoverable logic");
            //Add any Recovery Code here.
            kafkaTemplate.send(retryTopic, record.partition(), record.key(), record.value());

        } else {
            System.out.println("Inside the non recoverable logic and skipping the record : {}"+ record);
            kafkaTemplate.send(deadLetterTopic, record.partition(), record.key(), record.value());

        }
    };

    public DeadLetterPublishingRecoverer publishingRecoverer() {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r, e) -> {
                    System.out.println("Exception in publishingRecoverer : {} "+ e.getMessage());
                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        return new TopicPartition(retryTopic, r.partition());
                    }
                    else {
                        return new TopicPartition(deadLetterTopic, r.partition());
                    }
                });

        return recoverer;
    }

    //@Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory
                .getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(this.properties.buildConsumerProperties(null))));
        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);

        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }
}
