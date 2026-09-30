package com.coffeeandcode.kafkalab.producer;

import com.coffeeandcode.kafkalab.config.KafkaTopicConfig;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


@Service
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;


    public OrderEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendWithoutKey(String event){
        kafkaTemplate.send(KafkaTopicConfig.ORDER_TOPIC, event)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        log.error("Failed key=null value={}", event, error);
                        return;
                    }

                    RecordMetadata metadata = result.getRecordMetadata();

                    log.info("SENT  Key=null value={} --> partition={} offset={}",
                            event, metadata.partition(), metadata.offset());

                });
    }

    public void sendWithKey(String customerId,String event){
        kafkaTemplate.send(KafkaTopicConfig.ORDER_TOPIC,customerId ,event)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        log.error("Failed key={} value={}", customerId,event, error);
                        return;
                    }

                    RecordMetadata metadata = result.getRecordMetadata();

                    log.info("SENT  Key={} value={} --> partition={} offset={}",
                            customerId,event, metadata.partition(), metadata.offset());

                });
    }

}
