package com.github.h3yow.reliablemessagequeuejava.queue.model;

/**
 * 死信记录。
 */
public record DeadLetter(String deadLetterId,
                         String group,
                         String topic,
                         int partition,
                         long originalOffset,
                         long seq,
                         String partitionKey,
                         String originalMessageId,
                         byte[] payload,
                         int attempts,
                         String lastError,
                         long deadAt,
                         boolean replayed,
                         Long replayedToOffset,
                         String replayedToMessageId) {
}
