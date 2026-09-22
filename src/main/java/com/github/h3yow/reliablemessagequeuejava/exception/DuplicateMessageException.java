package com.github.h3yow.reliablemessagequeuejava.exception;

/**
 * 幂等键重复：同一主题下相同幂等键不得产生第二次可见投递。
 */
public class DuplicateMessageException extends MqException {
    public DuplicateMessageException(String message) {
        super(message);
    }
}
