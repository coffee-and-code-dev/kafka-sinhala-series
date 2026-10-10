package com.coffeeandcode.kafka_springboot.service;

import com.coffeeandcode.kafka_springboot.exception.InvalidOrderException;
import com.coffeeandcode.kafka_springboot.exception.InventoryServiceException;
import com.coffeeandcode.kafka_springboot.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Pretend business logic. The product name decides what happens:
 *   TEMP-ITEM  -> inventory service fails twice, then works
 *   BROKEN-ITEM -> inventory service is always down
 *   amount <= 0 -> invalid order (bad data)
 */

@Service
public class OrderProcessingService {

    private static final Logger log = LoggerFactory.getLogger(OrderProcessingService.class);

    private final Map<String, Integer> attempts = new ConcurrentHashMap<>();

    public void process(OrderEvent order) {
        if (order.amount() == null || order.amount().signum() <= 0) {
            throw new InvalidOrderException("Invalid amount " + order.amount() + " for " + order.orderId());
        }

        if ("BROKEN-ITEM".equals(order.product())) {
            throw new InventoryServiceException("Inventory service is down for " + order.orderId());
        }

        if ("TEMP-ITEM".equals(order.product())) {
            int attempt = attempts.merge(order.orderId(), 1, Integer::sum);
            if (attempt < 3) {
                throw new InventoryServiceException("Inventory service timeout for " + order.orderId());
            }
        }

        log.info("PROCESSED orderId={} customer={} product={} amount={}",
                order.orderId(), order.customerId(), order.product(), order.amount());
    }

}
