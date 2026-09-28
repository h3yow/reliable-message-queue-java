package com.github.h3yow.reliablemessagequeuejava.queue.model;

import java.util.List;

/**
 * 分区卡点聚合视图：只给定主题与分区（消费组可省略）时，返回该分区上
 * <em>每个消费组各自</em>的卡点信息。
 *
 * <p>{@code blockers} 里的每个元素是该消费组在该分区上的队首卡点
 * （口径与 {@link PartitionBlocker} 完全一致）；所有消费组都没有卡点时
 * {@code blocked=false} 且 {@code blockers} 为空列表。
 *
 * @param topic     主题
 * @param partition 分区
 * @param blocked   该分区上是否存在任意消费组的卡点
 * @param blockers  各消费组的卡点列表，按消费组名升序，保证结果可复现
 */
public record PartitionBlockerReport(String topic,
                                     int partition,
                                     boolean blocked,
                                     List<PartitionBlocker> blockers) {

    public static PartitionBlockerReport of(String topic, int partition,
                                            List<PartitionBlocker> blockers) {
        return new PartitionBlockerReport(topic, partition, !blockers.isEmpty(),
                List.copyOf(blockers));
    }

    public static PartitionBlockerReport clear(String topic, int partition) {
        return new PartitionBlockerReport(topic, partition, false, List.of());
    }
}
