package com.learn.self.kafka.product.email.notification.exception;

public class NonRetriableException extends RuntimeException {

    public NonRetriableException(String message) {
        super(message);
    }

    public NonRetriableException(Throwable cause) {
        super(cause);
    }
}
