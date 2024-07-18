package com.example.kafka.demo.consumer;

import com.example.kafka.demo.exception.TestException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import static org.springframework.kafka.retrytopic.TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE;
import static org.springframework.kafka.support.KafkaHeaders.RECEIVED_TOPIC;

@Component
@Slf4j
public class KafkaConsumer {

    @KafkaListener(
            topics = "message",
            groupId = "message-group"
    )

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 10000),
            topicSuffixingStrategy = SUFFIX_WITH_INDEX_VALUE,
            retryTopicSuffix = "-retrytopic",
            dltTopicSuffix = "-dlttopic",
            include = {TestException.class}
    )

    public void consume(String message, @Header(RECEIVED_TOPIC) String topic) {
        if (message.contains("fail")) {
            throw new TestException("Failed to consume");
        } else {
            log.info("Message consumed. Message: {}, Topic: {}", message, topic);
        }
    }

    @DltHandler
    public void deadLetter(String message, @Header(RECEIVED_TOPIC) String topic) {
        log.info("Message failed to consume: {} and topic : {}", message, topic);
    }
}
