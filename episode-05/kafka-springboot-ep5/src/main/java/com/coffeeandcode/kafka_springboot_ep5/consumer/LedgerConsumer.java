package com.coffeeandcode.kafka_springboot_ep5.consumer;

import com.coffeeandcode.kafka_springboot_ep5.config.KafkaTopicConfig;
import com.coffeeandcode.kafka_springboot_ep5.model.PaymentCreatedEvent;
import com.coffeeandcode.kafka_springboot_ep5.model.ProcessedEvent;
import com.coffeeandcode.kafka_springboot_ep5.repository.PaymentRepository;
import com.coffeeandcode.kafka_springboot_ep5.repository.ProcessedEventRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Component
public class LedgerConsumer {

    private static final Logger log = LoggerFactory.getLogger(LedgerConsumer.class);

    private final ProcessedEventRepository processedEventRepository;
    private final PaymentRepository paymentRepository;
    private final JsonMapper jsonMapper;

    public LedgerConsumer(ProcessedEventRepository processedEventRepository,
                          PaymentRepository paymentRepository,
                          JsonMapper jsonMapper) {
        this.processedEventRepository = processedEventRepository;
        this.paymentRepository = paymentRepository;
        this.jsonMapper = jsonMapper;
    }

    @KafkaListener(topics = KafkaTopicConfig.PAYMENT_EVENTS_TOPIC, groupId = "ledger-service")
    @Transactional
    public void consume(ConsumerRecord<String, String> record) {
        PaymentCreatedEvent event = jsonMapper.readValue(record.value(), PaymentCreatedEvent.class);

        // Seen this eventId before? Then it's a duplicate - do nothing.
        if (processedEventRepository.existsById(event.eventId())) {
            log.warn("DUPLICATE eventId={} paymentId={} -> already processed, skipped",
                    event.eventId(), event.paymentId());
            return;
        }

        if (paymentRepository.existsById(event.paymentId())) {
            log.info("LEDGER   recorded paymentId={} customer={} amount={} (partition={} offset={})",
                    event.paymentId(), event.customerId(), event.amount(), record.partition(), record.offset());
        } else {
            log.error("GHOST    got an event for paymentId={} but that payment does NOT exist in the DB!",
                    event.paymentId());
        }

        // Same transaction as the business work -> "processed" is saved only if the work succeeded
        processedEventRepository.save(new ProcessedEvent(event.eventId()));
    }
}
