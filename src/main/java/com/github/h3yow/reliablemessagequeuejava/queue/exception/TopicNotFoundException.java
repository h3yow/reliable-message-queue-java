package com.github.h3yow.reliablemessagequeuejava.queue.exception;

public class TopicNotFoundException extends QueueException {
    public TopicNotFoundException(String message) {
        super("TOPIC_NOT_FOUND", message);
    }
}
