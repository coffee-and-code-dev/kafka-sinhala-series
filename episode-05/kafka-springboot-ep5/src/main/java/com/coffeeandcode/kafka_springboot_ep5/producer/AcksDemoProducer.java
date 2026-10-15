package com.coffeeandcode.kafka_springboot_ep5.producer;

import com.coffeeandcode.kafka_springboot_ep5.config.KafkaTopicConfig;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


@Service
public class AcksDemoProducer {

    private static final Logger log = LoggerFactory.getLogger(AcksDemoProducer.class);

    private final Map<String, DefaultKafkaProducerFactory<String, String>> factories = new LinkedHashMap<>();
    private final Map<String, KafkaTemplate<String, String>> templates = new LinkedHashMap<>();

    public AcksDemoProducer(ProducerFactory<?, ?> producerFactory) {
        Map<String, Object> base = producerFactory.getConfigurationProperties();
        for (String acks : List.of("0", "1", "all")) {
            Map<String, Object> props = new HashMap<>(base);
            props.put(ProducerConfig.ACKS_CONFIG, acks);
            // The idempotent producer only works with acks=all, so switch it off for 0 and 1
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "all".equals(acks));
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "acks-" + acks + "-producer");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            // Short timeouts so a failure shows up in ~8 seconds instead of the default 2 minutes
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
            props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 8000);
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 8000);

            DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(props);
            factories.put(acks, factory);
            templates.put(acks, new KafkaTemplate<>(factory));
        }
    }

    public void send(String acks, String message) {
        KafkaTemplate<String, String> template = templates.get(acks);
        if (template == null) {
            throw new IllegalArgumentException("acks must be one of " + templates.keySet());
        }

        long start = System.currentTimeMillis();
        template.send(KafkaTopicConfig.ACKS_DEMO_TOPIC, message)
                .whenComplete((result, ex) -> {
                    long took = System.currentTimeMillis() - start;
                    if (ex != null) {
                        Throwable cause = NestedExceptionUtils.getMostSpecificCause(ex);
                        log.error("FAILED   acks={} message='{}' after {} ms -> {}: {}",
                                acks, message, took, cause.getClass().getSimpleName(), cause.getMessage());
                        return;
                    }
                    // With acks=0 the broker never answers, so the offset is unknown (-1)
                    RecordMetadata meta = result.getRecordMetadata();
                    log.info("SENT     acks={} message='{}' -> partition={} offset={} ({} ms)",
                            acks, message, meta.partition(), meta.offset(), took);
                });
    }

    @PreDestroy
    void close() {
        factories.values().forEach(DefaultKafkaProducerFactory::destroy);
    }
}