package com.github.h3yow.reliablemessagequeuejava.model;

/**
 * 死信条目（消费组维度）。
 */
public record DeadLetterEntry(
        String deadLetterId,
        String group,
        Message message,
        int partition,
        long offset,
        int attempts,
        String lastError,
        long deadAt
) {
}
