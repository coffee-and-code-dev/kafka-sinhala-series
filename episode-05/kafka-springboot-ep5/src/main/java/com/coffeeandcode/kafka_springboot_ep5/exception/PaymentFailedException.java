package com.coffeeandcode.kafka_springboot_ep5.exception;

public class PaymentFailedException extends RuntimeException {
    public PaymentFailedException(String message) {
        super(message);
    }
}