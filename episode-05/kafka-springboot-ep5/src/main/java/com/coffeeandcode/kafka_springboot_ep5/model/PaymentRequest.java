package com.coffeeandcode.kafka_springboot_ep5.model;

import java.math.BigDecimal;

public record PaymentRequest(String customerId, BigDecimal amount) {
}
