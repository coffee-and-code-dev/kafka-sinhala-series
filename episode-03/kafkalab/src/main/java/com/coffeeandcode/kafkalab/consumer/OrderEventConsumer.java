package com.coffeeandcode.kafkalab.consumer;


import com.coffeeandcode.kafkalab.config.KafkaTopicConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);


    @KafkaListener(topics = KafkaTopicConfig.ORDER_TOPIC,groupId = "order-service")
    public void consume(ConsumerRecord<String, String> orderRecord) {
        log.info("RECEIVED key={} value={} <-- partition={} offset={}",
                orderRecord.key(), orderRecord.value(), orderRecord.partition(), orderRecord.offset());
    }

}
