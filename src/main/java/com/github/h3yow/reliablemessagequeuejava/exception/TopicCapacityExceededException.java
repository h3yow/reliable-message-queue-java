package com.github.h3yow.reliablemessagequeuejava.exception;

public class TopicCapacityExceededException extends MqException {
    public TopicCapacityExceededException(String message) {
        super(message);
    }

    public TopicCapacityExceededException(String message, Throwable cause) {
        super(message, cause);
    }
}
