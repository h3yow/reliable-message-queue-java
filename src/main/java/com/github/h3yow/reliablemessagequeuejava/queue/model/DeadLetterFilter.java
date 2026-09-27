package com.github.h3yow.reliablemessagequeuejava.queue.model;

/**
 * 批量重放时筛选死信的条件。
 *
 * @param topic      必填主题
 * @param group      可选消费组过滤（null 表示不限消费组）
 * @param partition  可选分区过滤（null 表示不限分区）
 * @param deadAfter  可选，仅包含进入死信时间 &gt;= deadAfter 的记录（null 表示不限）
 * @param deadBefore 可选，仅包含进入死信时间 &lt;= deadBefore 的记录（null 表示不限）
 */
public record DeadLetterFilter(String topic,
                               String group,
                               Integer partition,
                               Long deadAfter,
                               Long deadBefore) {

    public static DeadLetterFilter forTopic(String topic) {
        return new DeadLetterFilter(topic, null, null, null, null);
    }

    public boolean matches(DeadLetter d) {
        if (topic != null && !topic.equals(d.topic())) {
            return false;
        }
        if (group != null && !group.equals(d.group())) {
            return false;
        }
        if (partition != null && partition != d.partition()) {
            return false;
        }
        if (deadAfter != null && d.deadAt() < deadAfter) {
            return false;
        }
        if (deadBefore != null && d.deadAt() > deadBefore) {
            return false;
        }
        return true;
    }
}
