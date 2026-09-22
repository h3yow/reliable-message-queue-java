package com.github.h3yow.reliablemessagequeuejava.model;

/**
 * 未确认消息的持久化状态（含重试退避）。
 */
public record InFlightLease(
        String deliveryId,
        String group,
        String topic,
        int partition,
        long offset,
        int attempt,
        long leasedAt,
        long leasedUntil,
        long backoffUntil,
        String lastError
) {
}
