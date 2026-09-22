package com.github.h3yow.reliablemessagequeuejava.queue.exception;

public class InvalidTopicException extends QueueException {
    public InvalidTopicException(String message) {
        super("INVALID_TOPIC", message);
    }
}
