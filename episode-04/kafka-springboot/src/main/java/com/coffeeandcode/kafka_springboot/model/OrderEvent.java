package com.coffeeandcode.kafka_springboot.model;

import java.math.BigDecimal;

public record OrderEvent(
        String orderId,
        String customerId,
        String product,
        BigDecimal amount,
        String status

) { }
