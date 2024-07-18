package com.example.kafka.demo.controller;

import com.example.kafka.demo.producer.KafkaProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test")
@RequiredArgsConstructor
public class TestController {

    private final KafkaProducer kafkaProducer;

    @GetMapping("/{message}")
    public void test(@PathVariable String message) {
        kafkaProducer.sendMessage(message);
    }
}
