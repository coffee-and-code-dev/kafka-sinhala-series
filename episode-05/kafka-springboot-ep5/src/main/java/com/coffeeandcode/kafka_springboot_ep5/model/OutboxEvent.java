package com.coffeeandcode.kafka_springboot_ep5.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    @Id
    private String id;               // the eventId
    private String aggregateType;    // e.g. "Payment"
    private String aggregateId;      // e.g. the paymentId
    private String eventType;        // e.g. "PaymentCreated"
    private String topic;            // where the relay should send it
    private String messageKey;       // Kafka key -> keeps one customer's events in order
    @Column(length = 4000)
    private String payload;          // the event as JSON
    private Instant createdAt;
    private Instant publishedAt;     // null = not sent to Kafka yet

    protected OutboxEvent() {

    }

    public OutboxEvent(String id, String aggregateType, String aggregateId, String eventType,
                       String topic, String messageKey, String payload) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.topic = topic;
        this.messageKey = messageKey;
        this.payload = payload;
        this.createdAt = Instant.now();
    }

    public void markPublished() { this.publishedAt = Instant.now(); }
    public void markUnpublished() { this.publishedAt = null; }

    public String getId() { return id; }
    public String getAggregateType() { return aggregateType; }
    public String getAggregateId() { return aggregateId; }
    public String getEventType() { return eventType; }
    public String getTopic() { return topic; }
    public String getMessageKey() { return messageKey; }
    public String getPayload() { return payload; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getPublishedAt() { return publishedAt; }
}