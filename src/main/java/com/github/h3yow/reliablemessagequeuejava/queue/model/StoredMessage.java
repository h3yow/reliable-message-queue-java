package com.github.h3yow.reliablemessagequeuejava.queue.model;

/**
 * 持久化的消息实体。
 */
public record StoredMessage(String id,
                            String topic,
                            int partition,
                            long offset,
                            long seq,
                            String partitionKey,
                            String idempotencyKey,
                            byte[] payload,
                            long publishedAt,
                            String replayOf) {
}
