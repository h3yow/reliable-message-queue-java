package com.github.h3yow.reliablemessagequeuejava.queue.model;

/**
 * 死信重放结果。
 */
public record ReplayResult(DeadLetter deadLetter, PublishResult publishResult) {
}
