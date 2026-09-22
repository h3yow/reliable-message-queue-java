package com.github.h3yow.reliablemessagequeuejava.exception;

public class DuplicateAckException extends MqException {
    public DuplicateAckException(String message) {
        super(message);
    }
}
