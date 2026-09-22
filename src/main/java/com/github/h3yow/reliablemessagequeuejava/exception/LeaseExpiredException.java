package com.github.h3yow.reliablemessagequeuejava.exception;

/**
 * 租约已过期（消息已被重新投递），旧交付的确认/否认将被拒绝。
 */
public class LeaseExpiredException extends MqException {
    public LeaseExpiredException(String message) {
        super(message);
    }
}
