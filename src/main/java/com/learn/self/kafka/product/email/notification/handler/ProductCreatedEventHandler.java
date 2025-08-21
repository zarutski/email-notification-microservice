package com.learn.self.kafka.product.email.notification.handler;

import com.learn.self.kafka.product.core.ProductCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics = "product-created-events-topic")
// subscribes class to Kafka topics; routes messages to @KafkaHandler methods
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    @KafkaHandler
    // used inside a @KafkaListener class; dispatches messages to this method based on the payload type - ProductCreatedEvent messages only
    public void handle(ProductCreatedEvent createdEvent) {
        LOGGER.info("Received event: {}", createdEvent.getTitle());
    }
}
