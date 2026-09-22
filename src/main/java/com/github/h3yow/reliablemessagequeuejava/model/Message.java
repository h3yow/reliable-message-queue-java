package com.github.h3yow.reliablemessagequeuejava.model;

/**
 * 不可变消息。seq 是消息在所属分区内的单调序号（同时作为位点）。
 */
public record Message(
        String id,
        String topic,
        String partitionKey,
        String idempotencyKey,
        byte[] payload,
        int partition,
        long seq,
        long publishedAt
) {
}
