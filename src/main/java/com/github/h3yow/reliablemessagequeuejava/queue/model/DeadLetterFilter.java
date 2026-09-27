package com.github.h3yow.reliablemessagequeuejava.queue.model;

/**
 * 死信批量筛选条件。
 *
 * @param topic      必填：按主题筛选
 * @param group      可空：按消费组筛选
 * @param partition  可空：按分区筛选
 * @param deadAtFrom 可空：进入死信时间下界（含，epoch 毫秒）
 * @param deadAtTo   可空：进入死信时间上界（含，epoch 毫秒）
 */
public record DeadLetterFilter(String topic,
                               String group,
                               Integer partition,
                               Long deadAtFrom,
                               Long deadAtTo) {

    public static DeadLetterFilter forTopic(String topic) {
        return new DeadLetterFilter(topic, null, null, null, null);
    }
}
