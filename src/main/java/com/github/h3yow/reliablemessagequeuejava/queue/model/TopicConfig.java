package com.github.h3yow.reliablemessagequeuejava.queue.model;

/**
 * 主题配置。
 */
public record TopicConfig(String name, int partitionCount, int capacity, int maxPayloadBytes) {
}
