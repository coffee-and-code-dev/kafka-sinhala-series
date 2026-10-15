package com.coffeeandcode.kafka_springboot_ep5.controller;

import com.coffeeandcode.kafka_springboot_ep5.exception.PaymentFailedException;
import com.coffeeandcode.kafka_springboot_ep5.model.OutboxEvent;
import com.coffeeandcode.kafka_springboot_ep5.model.Payment;
import com.coffeeandcode.kafka_springboot_ep5.model.PaymentRequest;
import com.coffeeandcode.kafka_springboot_ep5.repository.OutboxEventRepository;
import com.coffeeandcode.kafka_springboot_ep5.repository.PaymentRepository;
import com.coffeeandcode.kafka_springboot_ep5.service.PaymentService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
public class PaymentController {

    private final PaymentService paymentService;
    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxEventRepository;

    public PaymentController(PaymentService paymentService,
                             PaymentRepository paymentRepository,
                             OutboxEventRepository outboxEventRepository) {
        this.paymentService = paymentService;
        this.paymentRepository = paymentRepository;
        this.outboxEventRepository = outboxEventRepository;
    }

    @PostMapping("/payments/dual-write")
    public Payment dualWrite(@RequestBody PaymentRequest request,
                             @RequestParam(defaultValue = "false") boolean fail) {
        return paymentService.createWithDualWrite(request, fail);
    }

    @PostMapping("/payments")
    public Payment outbox(@RequestBody PaymentRequest request,
                          @RequestParam(defaultValue = "false") boolean fail) {
        return paymentService.createWithOutbox(request, fail);
    }

    @GetMapping("/payments")
    public List<Payment> payments() {
        return paymentRepository.findAll();
    }

    @GetMapping("/outbox")
    public List<OutboxEvent> outboxEvents() {
        return outboxEventRepository.findAllByOrderByCreatedAtAsc();
    }

    @PostMapping("/outbox/{eventId}/republish")
    public String republish(@PathVariable String eventId) {
        paymentService.republish(eventId);
        return "Event " + eventId + " will be sent again";
    }

    @ExceptionHandler(PaymentFailedException.class)
    public ResponseEntity<String> handle(PaymentFailedException ex) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("FAILED: " + ex.getMessage());
    }
}
