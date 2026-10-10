package com.coffeeandcode.kafka_springboot.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {


    public static final String ORDERS_TOPIC="orders";
    public static final String ORDERS_DLT_TOPIC="orders-dlt";
    public static final String NOTIFICATIONS_TOPIC="order-notifications";


    @Bean
    public NewTopic ordersTopic(){
        return TopicBuilder.name(ORDERS_TOPIC).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic ordersDltTopic(){
        return TopicBuilder.name(ORDERS_DLT_TOPIC).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic notificationsTopic(){
        return TopicBuilder.name(NOTIFICATIONS_TOPIC).partitions(3).replicas(1).build();
    }


}
