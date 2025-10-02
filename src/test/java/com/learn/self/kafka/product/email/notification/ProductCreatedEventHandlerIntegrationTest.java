package com.learn.self.kafka.product.email.notification;

import com.learn.self.kafka.product.core.ProductCreatedEvent;
import com.learn.self.kafka.product.email.notification.handler.ProductCreatedEventHandler;
import com.learn.self.kafka.product.email.notification.persistence.entity.ProcessedEventEntity;
import com.learn.self.kafka.product.email.notification.persistence.entity.repository.ProcessedEventRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ActiveProfiles("test")
@EmbeddedKafka
@SpringBootTest(properties = "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}")
public class ProductCreatedEventHandlerIntegrationTest {

    // в интеграционном тесте идепотенного консьюмера нужно проверить, что после записи сообщения в брокер, метод handle был вызван с ожидаемыми параметрами

    @MockitoBean
    private ProcessedEventRepository eventRepository;

    @MockitoBean
    private RestTemplate restTemplate;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @MockitoSpyBean
    private ProductCreatedEventHandler productCreatedEventHandler;

    @Test
    public void testProductCreatedEventProcessingSuccessful() throws ExecutionException, InterruptedException {
        // data preparation
        ProductCreatedEvent createdEvent = new ProductCreatedEvent();
        createdEvent.setProductId(UUID.randomUUID().toString());
        createdEvent.setTitle("Sam Sung");
        createdEvent.setPrice(new BigDecimal(200));
        createdEvent.setQuantity(1);

        String messageId = UUID.randomUUID().toString();
        String messageKey = createdEvent.getProductId();

        ProducerRecord<String, Object> record = new ProducerRecord<>(
                "product-created-events-topic",
                messageKey,
                createdEvent
        );
        record.headers().add("messageId", messageId.getBytes());
        record.headers().add(KafkaHeaders.KEY, messageId.getBytes());

        mockServicesInteractions();

        // sending message to the kafka topic: message is sent to the embedded Kafka topic (with `auto-offset-reset=earliest` consumers can read it even if they start after the message is produced)
        kafkaTemplate.send(record).get();

        // argumentCaptors capture the actual arguments passed to the handler
        ArgumentCaptor<String> messageIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageKeyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ProductCreatedEvent> eventCaptor = ArgumentCaptor.forClass(ProductCreatedEvent.class);

        // ensure the consumer is configured with `auto-offset-reset=earliest` so it reads messages from the beginning of the topic (otherwise the handler may miss messages that were produced before the consumer started)
        verify(productCreatedEventHandler, timeout(5000).times(1)).handle(eventCaptor.capture(), messageIdCaptor.capture(), messageKeyCaptor.capture());

        assertEquals(messageId, messageIdCaptor.getValue());
        assertEquals(messageKey, messageKeyCaptor.getValue());
        assertEquals(createdEvent.getProductId(), eventCaptor.getValue().getProductId());
    }

    private void mockServicesInteractions() {
        ProcessedEventEntity eventEntity = new ProcessedEventEntity();
        when(eventRepository.findByMessageId(anyString())).thenReturn(eventEntity);
        when(eventRepository.save(any(ProcessedEventEntity.class))).thenReturn(null);

        String responseBody = "{\"key\":\"value\"}";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> responseEntity = new ResponseEntity<>(responseBody, headers, HttpStatus.OK);

        when(restTemplate.exchange(
                any(String.class),
                any(HttpMethod.class),
                isNull(), eq(String.class)
        )).thenReturn(responseEntity);
    }

}
