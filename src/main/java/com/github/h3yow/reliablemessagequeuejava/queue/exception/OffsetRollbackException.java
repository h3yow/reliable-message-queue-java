package com.github.h3yow.reliablemessagequeuejava.queue.exception;

public class OffsetRollbackException extends QueueException {
    public OffsetRollbackException(String message) {
        super("OFFSET_ROLLBACK", message);
    }
}
