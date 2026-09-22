package com.github.h3yow.reliablemessagequeuejava.tests;

import com.github.h3yow.reliablemessagequeuejava.config.QueueProperties;
import com.github.h3yow.reliablemessagequeuejava.core.MessageQueue;
import com.github.h3yow.reliablemessagequeuejava.model.DeadLetterEntry;
import com.github.h3yow.reliablemessagequeuejava.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.model.PublishReceipt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/** 失败重试、退避上限、死信与重放。 */
class RetryAndDlqTest {

    @TempDir
    Path tmp;

    /** 可控时钟：VisibilityTimeout=5s 保证测试窗口内不超时；退避用虚拟时间推进。 */
    private static QueueProperties fastBackoffProps(Path dir) {
        return TestQueues.properties(dir, Duration.ofSeconds(5), 3,
                Duration.ofMillis(10), Duration.ofMillis(40));
    }

    @Test
    void failedMessageRetriesWithBackoffAndEntersDlq() {
        AtomicLong now = new AtomicLong(1_000_000L);
        QueueProperties props = fastBackoffProps(tmp);
        try (MessageQueue q = new MessageQueue(props,
                new com.github.h3yow.reliablemessagequeuejava.storage.PersistentStore(tmp.toString()),
                now::get)) {
            q.createTopic("orders", 1, 1000, 1024);
            q.publish("orders", "k1", "idem-1", "hello".getBytes());

            // attempt 1 交付 -> nack，退避 10ms
            Delivery d1 = q.poll("grp-a", "orders").orElseThrow();
            assertEquals(1, d1.attempt());
            assertEquals(1, q.nack("grp-a", d1.deliveryId(), "boom-1"));

            // 退避未到：窗口不可交付
            now.addAndGet(9);
            assertTrue(q.poll("grp-a", "orders").isEmpty(), "退避期间不得重新交付");

            // 退避到 -> attempt 2，指数退避 20ms
            now.addAndGet(2);
            Delivery d2 = q.poll("grp-a", "orders").orElseThrow();
            assertEquals(2, d2.attempt());
            assertEquals(d1.offset(), d2.offset());
            assertEquals(2, q.nack("grp-a", d2.deliveryId(), "boom-2"));

            now.addAndGet(19);
            assertTrue(q.poll("grp-a", "orders").isEmpty(), "退避期间不得重新交付");
            now.addAndGet(2);
            Delivery d3 = q.poll("grp-a", "orders").orElseThrow();
            assertEquals(3, d3.attempt());
            // 达到 maxAttempts=3：nack 返回 -1，进入死信，位点推进到 0
            assertEquals(-1, q.nack("grp-a", d3.deliveryId(), "boom-3"));

            List<DeadLetterEntry> dlq = q.deadLetters("grp-a", "orders");
            assertEquals(1, dlq.size());
            DeadLetterEntry e = dlq.get(0);
            assertEquals(3, e.attempts());
            assertEquals("boom-3", e.lastError());
            assertEquals(0, e.offset());
            assertEquals("k1", e.message().partitionKey());
            assertEquals(0L, q.committedOffset("grp-a", "orders", 0));

            // 旧 deliveryId 已失效
            assertThrows(com.github.h3yow.reliablemessagequeuejava.exception.LeaseExpiredException.class,
                    () -> q.ack("grp-a", d1.deliveryId()));
        }
    }

    @Test
    void otherPartitionsAreNotBlockedByFailingOne() {
        AtomicLong now = new AtomicLong(2_000_000L);
        QueueProperties props = fastBackoffProps(tmp);
        try (MessageQueue q = new MessageQueue(props,
                new com.github.h3yow.reliablemessagequeuejava.storage.PersistentStore(tmp.toString()),
                now::get)) {
            q.createTopic("orders", 2, 1000, 1024);
            q.publish("orders", "fail-key", null, "poison".getBytes());
            Delivery poison = q.poll("grp-a", "orders").orElseThrow();
            int poisonPartition = poison.partition();
            q.nack("grp-a", poison.deliveryId(), "fail"); // 进入退避，占用该分区窗口

            // 持续向两个分区发消息；另一个分区必须持续可消费
            for (int i = 0; i < 6; i++) {
                String key = "key-" + i;
                q.publish("orders", key, "idem-" + i, ("m" + i).getBytes());
                Optional<Delivery> d = q.poll("grp-a", "orders");
                if (d.isPresent() && d.get().partition() != poisonPartition) {
                    q.ack("grp-a", d.get().deliveryId());
                }
            }

            long otherPartition = poisonPartition == 0 ? 1 : 0;
            // 另一个分区至少交付并确认了消息，且位点推进
            assertTrue(q.committedOffset("grp-a", "orders", (int) otherPartition) >= 0,
                    "失败分区的退避不得阻塞其他分区消费");
        }
    }

    @Test
    void deadLetterReplayDoesNotBreakOffsetsOrIdempotency() {
        AtomicLong now = new AtomicLong(3_000_000L);
        QueueProperties props = fastBackoffProps(tmp);
        try (MessageQueue q = new MessageQueue(props,
                new com.github.h3yow.reliablemessagequeuejava.storage.PersistentStore(tmp.toString()),
                now::get)) {
            q.createTopic("orders", 1, 1000, 1024);
            q.publish("orders", "k1", "idem-1", "first".getBytes());
            Delivery d = q.poll("grp-a", "orders").orElseThrow();
            q.nack("grp-a", d.deliveryId(), "err1");
            now.addAndGet(50);
            Delivery d2 = q.poll("grp-a", "orders").orElseThrow();
            q.nack("grp-a", d2.deliveryId(), "err2");
            now.addAndGet(50);
            Delivery d3 = q.poll("grp-a", "orders").orElseThrow();
            q.nack("grp-a", d3.deliveryId(), "err3");

            DeadLetterEntry dlq = q.deadLetters("grp-a", "orders").get(0);
            long offsetBeforeReplay = q.committedOffset("grp-a", "orders", 0);
            assertEquals(0L, offsetBeforeReplay);

            // 重放：作为新消息追加，位点 1，历史位点不变
            PublishReceipt r1 = q.replayDeadLetter("grp-a", dlq.deadLetterId());
            assertEquals(1L, r1.offset());
            assertEquals(offsetBeforeReplay, q.committedOffset("grp-a", "orders", 0));

            // 重放消息可被重新消费（同分区队列尾部）
            Delivery redelivered = q.poll("grp-a", "orders").orElseThrow();
            assertEquals(1L, redelivered.offset());
            assertEquals("first", new String(redelivered.message().payload()));
            assertEquals(1, redelivered.attempt());
            q.ack("grp-a", redelivered.deliveryId());
            assertEquals(1L, q.committedOffset("grp-a", "orders", 0));

            // 重复重放同一死信：幂等，返回同一回执，不产生第二次可见投递
            PublishReceipt r2 = q.replayDeadLetter("grp-a", dlq.deadLetterId());
            assertEquals(r1.messageId(), r2.messageId());
            assertEquals(r1.offset(), r2.offset());
            assertTrue(q.poll("grp-a", "orders").isEmpty(),
                    "重复重放不得产生第二次可见投递");

            // 死信列表中已重放条目不重复列出
            assertEquals(0, q.deadLetters("grp-a", "orders").size());
        }
    }

    @Test
    void replayedDeadLetterStaysHiddenAfterRestart() {
        AtomicLong now = new AtomicLong(4_000_000L);
        QueueProperties props = fastBackoffProps(tmp);
        String dlqId;
        try (MessageQueue q = new MessageQueue(props,
                new com.github.h3yow.reliablemessagequeuejava.storage.PersistentStore(tmp.toString()),
                now::get)) {
            q.createTopic("rt", 1, 1000, 1024);
            q.publish("rt", "k", "idem-1", "x".getBytes());
            Delivery d = q.poll("grp-a", "rt").orElseThrow();
            q.nack("grp-a", d.deliveryId(), "e1");
            now.addAndGet(50);
            d = q.poll("grp-a", "rt").orElseThrow();
            q.nack("grp-a", d.deliveryId(), "e2");
            now.addAndGet(50);
            d = q.poll("grp-a", "rt").orElseThrow();
            q.nack("grp-a", d.deliveryId(), "e3");
            dlqId = q.deadLetters("grp-a", "rt").get(0).deadLetterId();
            q.replayDeadLetter("grp-a", dlqId);
            assertEquals(0, q.deadLetters("grp-a", "rt").size());
        }
        // 重启（新 store / 新队列重放日志）
        try (MessageQueue reopened = new MessageQueue(props,
                new com.github.h3yow.reliablemessagequeuejava.storage.PersistentStore(tmp.toString()),
                now::get)) {
            assertEquals(0, reopened.deadLetters("grp-a", "rt").size(),
                    "已重放死信在应用重启后仍不得再次列出");
        }
    }
}
