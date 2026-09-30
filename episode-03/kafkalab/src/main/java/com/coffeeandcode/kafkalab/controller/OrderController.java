package com.coffeeandcode.kafkalab.controller;

import com.coffeeandcode.kafkalab.producer.OrderEventProducer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderEventProducer orderEventProducer;

    public OrderController(OrderEventProducer orderEventProducer) {
        this.orderEventProducer = orderEventProducer;
    }


    @PostMapping("/no-key")
    public String sendWithoutKey(@RequestParam String event){
        orderEventProducer.sendWithoutKey(event);
        return "Sent without Key: " + event;
    }

    @PostMapping
    public String sendWithKey(@RequestParam String customerId,@RequestParam String event){
        orderEventProducer.sendWithKey(customerId, event);
        return "Sent with Key: " + customerId + " : " + event;
    }

}
