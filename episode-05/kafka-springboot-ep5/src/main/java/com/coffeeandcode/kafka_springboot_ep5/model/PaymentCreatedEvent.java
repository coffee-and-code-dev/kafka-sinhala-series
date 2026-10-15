package com.coffeeandcode.kafka_springboot_ep5.model;

import java.math.BigDecimal;

public record PaymentCreatedEvent(
        String eventId,
        String paymentId,
        String customerId,
        BigDecimal amount,
        String status) {

    public static PaymentCreatedEvent from(String eventId, Payment payment) {
        return new PaymentCreatedEvent(eventId, payment.getId(), payment.getCustomerId(),
                payment.getAmount(), payment.getStatus());
    }
}
