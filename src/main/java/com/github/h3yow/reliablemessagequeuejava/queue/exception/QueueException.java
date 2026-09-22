package com.github.h3yow.reliablemessagequeuejava.queue.exception;

/**
 * 队列相关异常基类，携带可区分的错误码。
 */
public class QueueException extends RuntimeException {
    private final String code;

    public QueueException(String code, String message) {
        super(message);
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
