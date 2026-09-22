package com.github.h3yow.reliablemessagequeuejava.queue.exception;

public class DuplicateAckException extends QueueException {
    public DuplicateAckException(String message) {
        super("DUPLICATE_ACK", message);
    }
}
