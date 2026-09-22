package com.github.h3yow.reliablemessagequeuejava.exception;

/**
 * 所有队列异常的基类，拒绝原因可通过具体子类区分。
 */
public class MqException extends RuntimeException {
    public MqException(String message) {
        super(message);
    }

    public MqException(String message, Throwable cause) {
        super(message, cause);
    }
}
