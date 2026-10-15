package com.coffeeandcode.kafka_springboot_ep5.service;

import com.coffeeandcode.kafka_springboot_ep5.model.OutboxEvent;
import com.coffeeandcode.kafka_springboot_ep5.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxRelay(OutboxEventRepository outboxEventRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 2000)
    @Transactional
    public void publishPending() {
        List<OutboxEvent> pending = outboxEventRepository.findTop50ByPublishedAtIsNullOrderByCreatedAtAsc();

        for (OutboxEvent event : pending) {
            try {
                // Wait for Kafka to confirm (acks=all) before we mark the row as sent
                kafkaTemplate.send(event.getTopic(), event.getMessageKey(), event.getPayload())
                        .get(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // Stop here (don't skip ahead) so events keep their order. Next run tries again.
                log.warn("RELAY    could not send event {} - will retry: {}",
                        event.getId(), NestedExceptionUtils.getMostSpecificCause(e).getMessage());
                return;
            }
            event.markPublished(); // saved when the transaction commits
            log.info("RELAY    published event {} ({}) key={}", event.getId(), event.getEventType(), event.getMessageKey());
        }
    }
}