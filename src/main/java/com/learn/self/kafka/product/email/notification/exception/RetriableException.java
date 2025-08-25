package com.learn.self.kafka.product.email.notification.exception;

public class RetriableException extends RuntimeException {

    public RetriableException(String message) {
        super(message);
    }

    public RetriableException(Throwable cause) {
        super(cause);
    }
}
