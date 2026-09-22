package com.github.h3yow.reliablemessagequeuejava.exception;

public class PayloadTooLargeException extends MqException {
    public PayloadTooLargeException(String message) {
        super(message);
    }
}
