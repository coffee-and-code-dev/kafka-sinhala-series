package com.coffeeandcode.kafka_springboot.exception;

/** A temporary problem with the email provider. Retrying might fix it. */
public class EmailServiceException extends RuntimeException {
    public EmailServiceException(String message) {
        super(message);
    }
}
