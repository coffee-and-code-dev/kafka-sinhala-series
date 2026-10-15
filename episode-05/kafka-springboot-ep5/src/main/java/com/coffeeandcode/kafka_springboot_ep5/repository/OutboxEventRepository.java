package com.coffeeandcode.kafka_springboot_ep5.repository;

import com.coffeeandcode.kafka_springboot_ep5.model.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {

    List<OutboxEvent> findTop50ByPublishedAtIsNullOrderByCreatedAtAsc();

    List<OutboxEvent> findAllByOrderByCreatedAtAsc();
}
