package com.coffeeandcode.kafka_springboot_ep5.controller;

import com.coffeeandcode.kafka_springboot_ep5.producer.AcksDemoProducer;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/acks")
public class AcksController {

    private final AcksDemoProducer producer;

    public AcksController(AcksDemoProducer producer) {
        this.producer = producer;
    }

    // POST /api/acks/all?message=hello   (acks = 0, 1 or all)
    @PostMapping("/{acks}")
    public String send(@PathVariable String acks, @RequestParam(defaultValue = "hello") String message) {
        producer.send(acks, message);
        return "Sending with acks=" + acks + " - check the console";
    }
}