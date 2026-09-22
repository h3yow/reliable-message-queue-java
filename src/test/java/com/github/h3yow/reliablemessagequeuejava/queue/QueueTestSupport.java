package com.github.h3yow.reliablemessagequeuejava.queue;

import com.github.h3yow.reliablemessagequeuejava.queue.model.Delivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * 测试公共工具：构造队列、打印统一字段日志。
 */
final class QueueTestSupport {

    private static final Logger log = LoggerFactory.getLogger(QueueTestSupport.class);

    private QueueTestSupport() {
    }

    static ReliableMessageQueue newQueue(Path dir, long leaseMillis, long backoffBase,
                                         long backoffMax, int maxAttempts) {
        return new ReliableMessageQueue(dir.resolve("queue-state.json"),
                leaseMillis, backoffBase, backoffMax, maxAttempts);
    }

    static ReliableMessageQueue reopen(Path dir, long leaseMillis, long backoffBase,
                                       long backoffMax, int maxAttempts) {
        return new ReliableMessageQueue(dir.resolve("queue-state.json"),
                leaseMillis, backoffBase, backoffMax, maxAttempts);
    }

    static String text(Delivery d) {
        return new String(d.payload(), StandardCharsets.UTF_8);
    }

    static String textPayload(byte[] payload) {
        return new String(payload, StandardCharsets.UTF_8);
    }

    static void logDelivery(String phase, Delivery d) {
        log.info("[{}] topic={} group={} key={} partition={} seq={} offset={} attempt={}",
                phase, d.topic(), d.group(), d.partitionKey(), d.partition(),
                d.seq(), d.offset(), d.attempt());
    }
}
