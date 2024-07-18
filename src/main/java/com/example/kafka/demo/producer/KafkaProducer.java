package com.example.kafka.demo.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String message) {
        log.info("Message sending : {}", message);
        kafkaTemplate.send("message", message);
        log.info("Message sent: {}", message);
    }
}
