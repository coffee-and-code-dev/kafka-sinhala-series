package com.coffeeandcode.kafka_springboot.exception;

/** Bad data. Retrying will never fix it, so it's NOT retryable. */
public class InvalidOrderException extends RuntimeException {
    public InvalidOrderException(String message) {
        super(message);
    }
}
