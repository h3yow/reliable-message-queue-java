package com.github.h3yow.reliablemessagequeuejava.queue.exception;

public class TopicCapacityExceededException extends QueueException {
    public TopicCapacityExceededException(String message) {
        super("TOPIC_CAPACITY_EXCEEDED", message);
    }
}
