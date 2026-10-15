package com.coffeeandcode.kafka_springboot_ep5.controller;

import com.coffeeandcode.kafka_springboot_ep5.consumer.PageViewConsumer;
import com.coffeeandcode.kafka_springboot_ep5.producer.PageViewProducer;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/page-views")
public class HotPartitionController {

    private final PageViewProducer producer;
    private final PageViewConsumer consumer;

    public HotPartitionController(PageViewProducer producer, PageViewConsumer consumer) {
        this.producer = producer;
        this.consumer = consumer;
    }

    // POST /api/page-views?count=500&salted=false
    @PostMapping
    public Map<Integer, Integer> send(@RequestParam(defaultValue = "500") int count,
                                      @RequestParam(defaultValue = "false") boolean salted) {
        return producer.send(count, salted);
    }

    // GET /api/page-views/stats -> messages processed so far, per partition
    @GetMapping("/stats")
    public Map<Integer, Integer> stats() {
        return consumer.stats();
    }

    @DeleteMapping("/stats")
    public String reset() {
        consumer.reset();
        return "Stats cleared";
    }
}
