package com.github.h3yow.reliablemessagequeuejava.queue.model;

/**
 * 分区卡点视图：<em>某个消费组</em>在某分区上当前挡住后续投递的第一条未解决消息。
 *
 * <p>只给定主题与分区、不指定消费组时，使用
 * {@link com.github.h3yow.reliablemessagequeuejava.queue.ReliableMessageQueue#getPartitionBlockers(String, String, int)}
 * 返回 {@link PartitionBlockerReport}，其中包含该分区上每个消费组各自的卡点。
 *
 * @param state 卡点状态：BACKOFF（退避等待中）/LEASED（在途租约）/LEASE_EXPIRED（租约超时未确认）/READY（可立即投递，如重启后尚未重新投递）
 */
public record PartitionBlocker(String topic,
                               String group,
                               int partition,
                               boolean blocked,
                               String state,
                               String messageId,
                               Long offset,
                               Long seq,
                               String partitionKey,
                               Integer attempts,
                               String lastError,
                               Long nextDeliverableAt,
                               Long leaseExpiresAt,
                               Long publishedAt) {

    public static PartitionBlocker notBlocked(String topic, String group, int partition) {
        return new PartitionBlocker(topic, group, partition, false, null, null, null, null,
                null, null, null, null, null, null);
    }
}
