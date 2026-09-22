package com.github.h3yow.reliablemessagequeuejava.model;

/**
 * 发布成功回执。
 */
public record PublishReceipt(
        String messageId,
        String topic,
        int partition,
        long offset
) {
}
