package com.coffeeandcode.kafka_springboot_ep5.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payments")
public class Payment {

    @Id
    private String id;
    private String customerId;
    private BigDecimal amount;
    private String status;
    private Instant createdAt;

    protected Payment() {

    }

    public Payment(String customerId, BigDecimal amount) {
        this.id = "PAY-" + UUID.randomUUID().toString().substring(0, 8);
        this.customerId = customerId;
        this.amount = amount;
        this.status = "CREATED";
        this.createdAt = Instant.now();
    }

    public String getId() { return id; }
    public String getCustomerId() { return customerId; }
    public BigDecimal getAmount() { return amount; }
    public String getStatus() { return status; }
    public Instant getCreatedAt() { return createdAt; }
}