package com.coffeeandcode.kafka_springboot_ep5.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String ACKS_DEMO_TOPIC = "acks-demo";
    public static final String PAGE_VIEWS_TOPIC = "page-views";
    public static final String PAYMENT_EVENTS_TOPIC = "payment-events";

    @Bean
    public NewTopic acksDemoTopic() {
        return replicatedTopic(ACKS_DEMO_TOPIC);
    }

    @Bean
    public NewTopic pageViewsTopic() {
        return replicatedTopic(PAGE_VIEWS_TOPIC);
    }

    @Bean
    public NewTopic paymentEventsTopic() {
        return replicatedTopic(PAYMENT_EVENTS_TOPIC);
    }

    private static NewTopic replicatedTopic(String name) {
        return TopicBuilder.name(name)
                .partitions(3)
                .replicas(3)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .build();
    }
}
