package com.coffeeandcode.kafka_springboot.consumer;

import com.coffeeandcode.kafka_springboot.config.KafkaTopicConfig;
import com.coffeeandcode.kafka_springboot.model.OrderEvent;
import com.coffeeandcode.kafka_springboot.service.NotificationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Component;

/**
 * Uses NON-BLOCKING retries: a failed message moves to a retry topic,
 * so the next messages in the partition are not blocked.
 */
@Component
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    private final NotificationService notificationService;

    public NotificationConsumer(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    // 4 attempts: main topic, then retry after 2s, 4s, 8s -> then the DLT
    @RetryableTopic(
            attempts = "4",
            backOff = @BackOff(delay = 2000, multiplier = 2),
            kafkaTemplate = "kafkaTemplate")
    @KafkaListener(topics = KafkaTopicConfig.NOTIFICATIONS_TOPIC, groupId = "notification-service")
    public void consume(ConsumerRecord<String, OrderEvent> record) {
        log.info("NOTIFY   topic={} key={} orderId={}",
                record.topic(), record.key(), record.value().orderId());
        notificationService.sendEmail(record.value());
    }

    @DltHandler
    public void handleDlt(ConsumerRecord<String, OrderEvent> record) {
        log.error("NOTIFY-DLT topic={} key={} orderId={} -> giving up, needs manual action",
                record.topic(), record.key(), record.value().orderId());
    }
}