package com.github.h3yow.reliablemessagequeuejava.model;

import java.time.Duration;

/**
 * 主题配置。
 */
public record TopicConfig(
        String name,
        int partitionCount,
        long capacity,
        int maxPayloadBytes,
        Duration retention
) {
}
