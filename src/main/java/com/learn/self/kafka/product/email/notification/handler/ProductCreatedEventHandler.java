package com.learn.self.kafka.product.email.notification.handler;

import com.learn.self.kafka.product.core.ProductCreatedEvent;
import com.learn.self.kafka.product.email.notification.exception.NonRetriableException;
import com.learn.self.kafka.product.email.notification.exception.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Component
@KafkaListener(topics = "product-created-events-topic")
// subscribes class to Kafka topics; routes messages to @KafkaHandler methods
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    private final RestTemplate restTemplate;

    public ProductCreatedEventHandler(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @KafkaHandler
    // used inside a @KafkaListener class; routes consumed messages to this method based on the payload type (ProductCreatedEvent messages only in this case)
    public void handle(ProductCreatedEvent createdEvent) {
        LOGGER.info("Received event: {}", createdEvent.getTitle());

        // simulate external service call (for testing retry)
        String url = "http://localhost:8090/response/200";
        try {
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, null, String.class);
            if (response.getStatusCode().value() == HttpStatus.OK.value()) {
                LOGGER.info("Received response: {}", response.getBody());
            }
        } catch (ResourceAccessException e) {
            LOGGER.error(e.getMessage());
            throw new RetriableException(e); // temporary network/resource issue -> retry makes sense because the problem may be resolved on the next attempt
        } catch (HttpServerErrorException e) {
            LOGGER.error(e.getMessage());
            throw new NonRetriableException(e); // server returned 5xx -> retry may not help; treat as non-retriable to avoid endless retries
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            throw new NonRetriableException(e); // any other unexpected exception -> retry unlikely to succeed, so consider non-retriable
        }
    }
}
