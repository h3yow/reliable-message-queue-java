package com.github.h3yow.reliablemessagequeuejava.queue.model;

/**
 * 投递给消费者的消息视图。
 */
public record Delivery(String topic,
                       String group,
                       int partition,
                       long offset,
                       long seq,
                       String partitionKey,
                       String messageId,
                       byte[] payload,
                       int attempt,
                       String leaseToken) {
}
