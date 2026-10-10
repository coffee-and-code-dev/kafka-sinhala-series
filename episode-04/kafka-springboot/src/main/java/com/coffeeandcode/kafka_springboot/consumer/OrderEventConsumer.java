package com.coffeeandcode.kafka_springboot.consumer;

import com.coffeeandcode.kafka_springboot.config.KafkaTopicConfig;
import com.coffeeandcode.kafka_springboot.model.OrderEvent;
import com.coffeeandcode.kafka_springboot.service.OrderProcessingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Uses BLOCKING retries (the DefaultErrorHandler bean) and the orders-dlt topic. */
@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final OrderProcessingService orderProcessingService;

    public OrderEventConsumer(OrderProcessingService orderProcessingService) {
        this.orderProcessingService = orderProcessingService;
    }

    @KafkaListener(topics = KafkaTopicConfig.ORDERS_TOPIC, groupId = "order-service")
    public void consume(ConsumerRecord<String, OrderEvent> record) {
        log.info("RECEIVED key={} partition={} offset={} thread={} order={}",
                record.key(), record.partition(), record.offset(),
                Thread.currentThread().getName(), record.value());


        orderProcessingService.process(record.value());
    }
}