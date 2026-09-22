package com.github.h3yow.reliablemessagequeuejava.queue.model;

/**
 * 发布结果。
 */
public record PublishResult(String messageId, int partition, long offset, long seq, boolean duplicate) {
}
