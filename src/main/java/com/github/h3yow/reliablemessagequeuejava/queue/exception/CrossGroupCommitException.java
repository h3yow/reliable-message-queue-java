package com.github.h3yow.reliablemessagequeuejava.queue.exception;

public class CrossGroupCommitException extends QueueException {
    public CrossGroupCommitException(String message) {
        super("CROSS_GROUP_COMMIT", message);
    }
}
