package com.coffeeandcode.kafka_springboot_ep5.repository;

import com.coffeeandcode.kafka_springboot_ep5.model.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
}