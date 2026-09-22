package com.github.h3yow.reliablemessagequeuejava.core;

import org.springframework.scheduling.annotation.Scheduled;

/**
 * 周期性回收租约超时的未确认消息，模拟消费者异常退出 / 消费超时后的重新投递。
 */
public class RedeliveryService {

    private final MessageQueue messageQueue;

    public RedeliveryService(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    @Scheduled(fixedDelayString = "${rmq.redelivery-interval-ms:1000}")
    public void reclaim() { /* TODO */ }
}
