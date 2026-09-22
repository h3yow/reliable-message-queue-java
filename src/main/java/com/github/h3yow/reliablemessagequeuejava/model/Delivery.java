package com.github.h3yow.reliablemessagequeuejava.model;

/**
 * 一次投递交付。attempt 从 1 开始；leasedUntil 为本次租约到期时间。
 */
public record Delivery(
        String deliveryId,
        String group,
        Message message,
        int partition,
        long offset,
        int attempt,
        long leasedAt,
        long leasedUntil
) {
}
