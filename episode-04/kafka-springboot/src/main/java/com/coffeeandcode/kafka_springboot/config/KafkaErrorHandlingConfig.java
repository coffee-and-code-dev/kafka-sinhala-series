package com.coffeeandcode.kafka_springboot.config;


import com.coffeeandcode.kafka_springboot.exception.InvalidOrderException;
import com.coffeeandcode.kafka_springboot.model.OrderEvent;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Bean
    public DefaultErrorHandler errorHandler(ProducerFactory<?, ?> producerFactory) {


        Map<Class<?>, Serializer<?>> serializers = Map.of(
                byte[].class, new ByteArraySerializer(),
                OrderEvent.class, new JacksonJsonSerializer<OrderEvent>());

        KafkaTemplate<String, Object> dltTemplate = new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<String, Object>(
                        producerFactory.getConfigurationProperties(),
                        new StringSerializer(),
                        new DelegatingByTypeSerializer(serializers)));


        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(dltTemplate,
                (record, ex) -> new TopicPartition(KafkaTopicConfig.ORDERS_DLT_TOPIC, record.partition()));


        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 3L));


        errorHandler.addNotRetryableExceptions(InvalidOrderException.class);


        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("FAILED   attempt={} key={} partition={} offset={} reason={}",
                        deliveryAttempt, record.key(), record.partition(), record.offset(),
                        NestedExceptionUtils.getMostSpecificCause(ex).getMessage()));


        errorHandler.setLogLevel(KafkaException.Level.DEBUG);

        return errorHandler;
    }
}