package com.coffeeandcode.kafka_springboot_ep5.producer;

import com.coffeeandcode.kafka_springboot_ep5.config.KafkaTopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

@Service
public class PageViewProducer {

    private static final Logger log = LoggerFactory.getLogger(PageViewProducer.class);

    public static final String HOT_KEY = "big-merchant";
    // With 3 partitions, big-merchant#0, #1 and #2 land on 3 different partitions
    private static final int SALT_BUCKETS = 3;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public PageViewProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /** @return how many messages went to each partition */
    public Map<Integer, Integer> send(int count, boolean salted) {
        List<CompletableFuture<SendResult<String, String>>> futures = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            // 1 in 5 messages comes from a normal small merchant, the rest from the big one
            String key = (i % 5 == 0) ? "merchant-" + i : hotKey(i, salted);
            futures.add(kafkaTemplate.send(KafkaTopicConfig.PAGE_VIEWS_TOPIC, key, "view-" + i));
        }

        Map<Integer, Integer> perPartition = new TreeMap<>();
        for (CompletableFuture<SendResult<String, String>> future : futures) {
            int partition = future.join().getRecordMetadata().partition();
            perPartition.merge(partition, 1, Integer::sum);
        }

        log.info("SENT     {} page views (salted={}) -> messages per partition {}", count, salted, perPartition);
        return perPartition;
    }

    // Key salting: big-merchant -> big-merchant#0 / #1 / #2
    private static String hotKey(int i, boolean salted) {
        return salted ? HOT_KEY + "#" + (i % SALT_BUCKETS) : HOT_KEY;
    }
}