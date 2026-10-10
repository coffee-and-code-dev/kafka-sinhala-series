package com.coffeeandcode.kafka_springboot.controller;


import com.coffeeandcode.kafka_springboot.model.OrderEvent;
import com.coffeeandcode.kafka_springboot.producer.OrderEventProducer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class OrderController {

    private final OrderEventProducer producer;

    public OrderController(OrderEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/orders")
    public String sendOrder(@RequestBody OrderEvent order) {
        producer.sendOrder(order);
        return "Order sent: " + order.orderId();
    }

    @PostMapping("/notifications")
    public String sendNotification(@RequestBody OrderEvent order) {
        producer.sendNotification(order);
        return "Notification sent: " + order.orderId();
    }

    @PostMapping("/orders/poison-pill")
    public String sendPoisonPill(@RequestParam(defaultValue = "cust-9") String key,
                                 @RequestBody String rawText) {
        producer.sendPoisonPill(key, rawText);
        return "Poison pill sent with key " + key;
    }
}