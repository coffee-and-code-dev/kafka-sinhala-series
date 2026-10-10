package com.coffeeandcode.kafka_springboot.service;

import com.coffeeandcode.kafka_springboot.exception.EmailServiceException;
import com.coffeeandcode.kafka_springboot.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Pretend email sender, same rules as OrderProcessingService. */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final Map<String, Integer> attempts = new ConcurrentHashMap<>();

    public void sendEmail(OrderEvent order) {
        if ("BROKEN-ITEM".equals(order.product())) {
            throw new EmailServiceException("Email provider is down for " + order.orderId());
        }

        if ("TEMP-ITEM".equals(order.product())) {
            int attempt = attempts.merge(order.orderId(), 1, Integer::sum);
            if (attempt < 3) {
                throw new EmailServiceException("Email provider timeout for " + order.orderId());
            }
        }

        log.info("EMAIL SENT orderId={} customer={}", order.orderId(), order.customerId());
    }
}
