package com.github.h3yow.reliablemessagequeuejava.queue.model;

import java.util.List;

/**
 * 分区卡点聚合视图：只给主题和分区（不指定消费组）时，返回该分区上每个消费组各自的卡点。
 *
 * @param topic     主题
 * @param partition 分区
 * @param blocked   该分区是否存在任意消费组的卡点
 * @param groups    每个被卡住的消费组对应的卡点视图（按消费组名排序）；无卡点时为空列表
 */
public record PartitionBlockers(String topic,
                                int partition,
                                boolean blocked,
                                List<PartitionBlocker> groups) {
}
