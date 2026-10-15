package com.coffeeandcode.kafka_springboot_ep5.service;

import com.coffeeandcode.kafka_springboot_ep5.config.KafkaTopicConfig;
import com.coffeeandcode.kafka_springboot_ep5.exception.PaymentFailedException;
import com.coffeeandcode.kafka_springboot_ep5.model.OutboxEvent;
import com.coffeeandcode.kafka_springboot_ep5.model.Payment;
import com.coffeeandcode.kafka_springboot_ep5.model.PaymentCreatedEvent;
import com.coffeeandcode.kafka_springboot_ep5.model.PaymentRequest;
import com.coffeeandcode.kafka_springboot_ep5.repository.OutboxEventRepository;
import com.coffeeandcode.kafka_springboot_ep5.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.util.UUID;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JsonMapper jsonMapper;

    public PaymentService(PaymentRepository paymentRepository,
                          OutboxEventRepository outboxEventRepository,
                          KafkaTemplate<String, String> kafkaTemplate,
                          JsonMapper jsonMapper) {
        this.paymentRepository = paymentRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.jsonMapper = jsonMapper;
    }

    /**
     * THE PROBLEM: the dual write.
     * Write #1 goes to the database, write #2 goes to Kafka.
     * They are NOT in the same transaction - Kafka knows nothing about our DB transaction.
     */
    @Transactional
    public Payment createWithDualWrite(PaymentRequest request, boolean failAfterSend) {
        Payment payment = paymentRepository.save(new Payment(request.customerId(), request.amount()));

        PaymentCreatedEvent event = PaymentCreatedEvent.from(UUID.randomUUID().toString(), payment);
        kafkaTemplate.send(KafkaTopicConfig.PAYMENT_EVENTS_TOPIC, payment.getCustomerId(),
                jsonMapper.writeValueAsString(event));
        log.info("DUAL     saved payment {} and sent event {} straight to Kafka", payment.getId(), event.eventId());

        if (failAfterSend) {
            // The DB rolls back... but the Kafka message is already gone. It can't be "un-sent".
            throw new PaymentFailedException("Simulated crash after the Kafka send - DB transaction rolled back");
        }
        return payment;
    }

    /**
     * THE FIX: the transactional outbox.
     * The payment AND the event go into the SAME database, in ONE transaction.
     * Both are saved, or neither is. The OutboxRelay sends the event to Kafka later.
     */
    @Transactional
    public Payment createWithOutbox(PaymentRequest request, boolean failAfterSave) {
        Payment payment = paymentRepository.save(new Payment(request.customerId(), request.amount()));

        PaymentCreatedEvent event = PaymentCreatedEvent.from(UUID.randomUUID().toString(), payment);
        outboxEventRepository.save(new OutboxEvent(
                event.eventId(), "Payment", payment.getId(), "PaymentCreated",
                KafkaTopicConfig.PAYMENT_EVENTS_TOPIC, payment.getCustomerId(),
                jsonMapper.writeValueAsString(event)));
        log.info("OUTBOX   saved payment {} + outbox event {} in ONE transaction", payment.getId(), event.eventId());

        if (failAfterSave) {
            // Both rows roll back together -> no payment, no event. Consistent.
            throw new PaymentFailedException("Simulated crash after saving - payment AND outbox event rolled back");
        }
        return payment;
    }

    /** Demo only: pretend the relay crashed after sending but before marking the row as sent. */
    @Transactional
    public void republish(String eventId) {
        OutboxEvent event = outboxEventRepository.findById(eventId)
                .orElseThrow(() -> new IllegalArgumentException("No outbox event " + eventId));
        event.markUnpublished();
        log.warn("OUTBOX   event {} marked as NOT sent -> the relay will send it again", eventId);
    }
}