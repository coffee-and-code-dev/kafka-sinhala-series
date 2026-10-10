package com.coffeeandcode.kafka_springboot.exception;

/** A temporary problem (timeout, service down). Retrying might fix it. */
public class InventoryServiceException extends RuntimeException {
    public InventoryServiceException(String message) {
        super(message);
    }
}
