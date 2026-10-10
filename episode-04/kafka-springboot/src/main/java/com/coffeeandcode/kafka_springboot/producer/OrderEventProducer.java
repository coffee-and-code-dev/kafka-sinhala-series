package com.coffeeandcode.kafka_springboot.producer;

import com.coffeeandcode.kafka_springboot.config.KafkaTopicConfig;
import com.coffeeandcode.kafka_springboot.model.OrderEvent;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Service;

@Service
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    // Only for the poison pill demo: sends plain text (not JSON) to the orders topic
    private final DefaultKafkaProducerFactory<String, String> rawProducerFactory;
    private final KafkaTemplate<String, String> rawTemplate;

    public OrderEventProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate,
                              ProducerFactory<?, ?> producerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.rawProducerFactory = new DefaultKafkaProducerFactory<>(
                producerFactory.getConfigurationProperties(), new StringSerializer(), new StringSerializer());
        this.rawTemplate = new KafkaTemplate<>(rawProducerFactory);
    }
    public void sendOrder(OrderEvent order) {
        send(KafkaTopicConfig.ORDERS_TOPIC, order);
    }

    public void sendNotification(OrderEvent order) {
        send(KafkaTopicConfig.NOTIFICATIONS_TOPIC, order);
    }



    private void send(String topic, OrderEvent order) {
        kafkaTemplate.send(topic,order.customerId(), order)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        log.error("FAILED   topic={} order={}", topic, order.orderId(), error);
                        return;
                    }
                    RecordMetadata meta = result.getRecordMetadata();
                    log.info("SENT     topic={} key={} orderId={} -> partition={} offset={}",
                            topic, order.customerId(), order.orderId(), meta.partition(), meta.offset());
                });
    }


    public void sendPoisonPill(String key, String rawText) {
        rawTemplate.send(KafkaTopicConfig.ORDERS_TOPIC, key, rawText)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("FAILED   poison pill not sent", ex);
                        return;
                    }
                    RecordMetadata meta = result.getRecordMetadata();
                    log.info("SENT     POISON PILL key={} value='{}' -> partition={} offset={}",
                            key, rawText, meta.partition(), meta.offset());
                });
    }

}
