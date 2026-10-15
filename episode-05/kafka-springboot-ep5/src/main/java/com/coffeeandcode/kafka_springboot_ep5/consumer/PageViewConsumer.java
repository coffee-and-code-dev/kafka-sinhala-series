package com.coffeeandcode.kafka_springboot_ep5.consumer;

import com.coffeeandcode.kafka_springboot_ep5.config.KafkaTopicConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class PageViewConsumer {

    private static final Logger log = LoggerFactory.getLogger(PageViewConsumer.class);
    private static final long PROCESSING_TIME_MS = 50;

    private final Map<Integer, AtomicInteger> processed = new ConcurrentHashMap<>();

    @KafkaListener(topics = KafkaTopicConfig.PAGE_VIEWS_TOPIC, groupId = "page-view-service", concurrency = "3")
    public void consume(ConsumerRecord<String, String> record) throws InterruptedException {
        Thread.sleep(PROCESSING_TIME_MS);

        int count = processed.computeIfAbsent(record.partition(), p -> new AtomicInteger()).incrementAndGet();
        if (count % 50 == 0) {
            log.info("PAGEVIEW partition={} processed={} lastKey={} thread={}",
                    record.partition(), count, record.key(), Thread.currentThread().getName());
        }
    }

    public Map<Integer, Integer> stats() {
        Map<Integer, Integer> result = new TreeMap<>();
        processed.forEach((partition, count) -> result.put(partition, count.get()));
        return result;
    }

    public void reset() {
        processed.clear();
    }
}