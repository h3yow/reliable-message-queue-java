package com.github.h3yow.reliablemessagequeuejava.queue.exception;

public class PayloadTooLargeException extends QueueException {
    public PayloadTooLargeException(String message) {
        super("PAYLOAD_TOO_LARGE", message);
    }
}
