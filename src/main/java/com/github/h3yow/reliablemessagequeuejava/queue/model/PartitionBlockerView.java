package com.github.h3yow.reliablemessagequeuejava.queue.model;

import java.util.List;

/**
 * 分区卡点查询结果：列出每个消费组视角下当前挡着分区投递的那一条消息。
 *
 * @param topic     主题
 * @param partition 分区
 * @param blocked   是否存在卡点（任何消费组有未解决的队头消息即为 true）
 * @param blockers  各消费组的卡点明细（无卡点时为空列表）
 */
public record PartitionBlockerView(String topic, int partition, boolean blocked,
                                   List<PartitionBlocker> blockers) {

    public static PartitionBlockerView notBlocked(String topic, int partition) {
        return new PartitionBlockerView(topic, partition, false, List.of());
    }
}
