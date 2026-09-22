package com.github.h3yow.reliablemessagequeuejava.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * 周期性扫描租约超时的未确认消息。
 *
 * <p>真正的重新投递在下次 {@link MessageQueue#poll} 时惰性发生；本服务负责让
 * "消费者异常退出 / 消费超时"在没有 poll 流量时也可被观测（日志 + 计数）。
 * 应用重启后，未确认租约由持久化日志恢复，同样由本机制再次交付。</p>
 */
public class RedeliveryService {

    private static final Logger log = LoggerFactory.getLogger(RedeliveryService.class);

    private final MessageQueue messageQueue;

    public RedeliveryService(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    @Scheduled(fixedDelayString = "${rmq.redelivery-interval-ms:1000}")
    public void reclaim() {
        int expired = messageQueue.reclaimExpiredLeases();
        if (expired > 0) {
            log.warn("检测到 {} 条租约超时的未确认消息，将在下次拉取时重新投递", expired);
        }
    }
}
