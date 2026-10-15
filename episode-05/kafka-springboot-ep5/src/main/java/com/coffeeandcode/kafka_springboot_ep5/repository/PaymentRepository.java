package com.coffeeandcode.kafka_springboot_ep5.repository;


import com.coffeeandcode.kafka_springboot_ep5.model.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRepository extends JpaRepository<Payment, String> {
}
