package com.coffeeandcode.kafka_springboot.consumer;

import com.coffeeandcode.kafka_springboot.config.KafkaTopicConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Reads the dead letter topic. The value is read as a plain String, so even
 * a poison pill (not valid JSON) can be read and logged here.
 */
@Component
public class OrderDltConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderDltConsumer.class);

    @KafkaListener(
            topics = KafkaTopicConfig.ORDERS_DLT_TOPIC,
            groupId = "order-dlt-service",
            properties = "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer")
    public void consume(ConsumerRecord<String, String> record) {
        log.error("DLT      key={} value={}", record.key(), record.value());
        log.error("DLT      original={}-{}@{} exception={}",
                text(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                number(record, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                number(record, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                text(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN));
        log.error("DLT      reason={}", text(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE));

        // In a real system: save to a DB table, raise an alert, build a replay tool...
    }

    private static String text(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? "-" : new String(header.value(), StandardCharsets.UTF_8);
    }

    // Partition is stored as a 4-byte int, offset as an 8-byte long
    private static String number(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            return "-";
        }
        ByteBuffer buffer = ByteBuffer.wrap(header.value());
        return header.value().length == Integer.BYTES
                ? String.valueOf(buffer.getInt())
                : String.valueOf(buffer.getLong());
    }
}
