package com.github.h3yow.reliablemessagequeuejava.queue.exception;

public class DuplicateIdempotencyKeyException extends QueueException {
    public DuplicateIdempotencyKeyException(String message) {
        super("DUPLICATE_IDEMPOTENCY_KEY", message);
    }
}
