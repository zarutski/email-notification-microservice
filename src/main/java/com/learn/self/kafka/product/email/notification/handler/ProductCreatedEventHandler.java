package com.learn.self.kafka.product.email.notification.handler;

import com.learn.self.kafka.product.core.ProductCreatedEvent;
import com.learn.self.kafka.product.email.notification.exception.NonRetriableException;
import com.learn.self.kafka.product.email.notification.exception.RetriableException;
import com.learn.self.kafka.product.email.notification.persistence.entity.ProcessedEventEntity;
import com.learn.self.kafka.product.email.notification.persistence.entity.repository.ProcessedEventRepository;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
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

    private final ProcessedEventRepository eventRepository;

    public ProductCreatedEventHandler(RestTemplate restTemplate, ProcessedEventRepository eventRepository) {
        this.restTemplate = restTemplate;
        this.eventRepository = eventRepository;
    }

    @Transactional // message processing should be performed within transaction (so messageId save is guaranteed after processing is finished)
    @KafkaHandler
    // used inside a @KafkaListener class; routes consumed messages to this method based on the payload type (ProductCreatedEvent messages only in this case)
    public void handle(@Payload ProductCreatedEvent createdEvent, // @Payload needs to be provided (helps to resolve message mapping)
                       @Header("messageId") String messageId, // @Header helps to resolve message mapping (custom header name)
                       @Header(KafkaHeaders.RECEIVED_KEY) String messageKey) { // @Header helps to resolve message mapping (default header name)
        LOGGER.info("Received event: {}, productId: {}", createdEvent.getTitle(), createdEvent.getProductId());

        // --- idempotent consumer implementation start ---
        // check if messageId is already present in DB
        ProcessedEventEntity eventEntity = eventRepository.findByMessageId(messageId);
        if (eventEntity != null) {
            // message was already processed
            LOGGER.info("Duplicate message id: {}", messageId);
            return;
        }
        // --- idempotent consumer implementation pause ---

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

        // --- idempotent consumer implementation proceed ---
        try {
            eventRepository.save(new ProcessedEventEntity(messageId, createdEvent.getProductId())); // saving messageId is the final step of idempotent consumer logic
        } catch (DataIntegrityViolationException e) {
            LOGGER.error(e.getMessage());
            throw new NonRetriableException(e);
        }
        // --- idempotent consumer implementation finish ---
    }
}
