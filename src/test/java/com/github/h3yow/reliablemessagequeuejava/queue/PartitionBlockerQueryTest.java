package com.github.h3yow.reliablemessagequeuejava.queue;

import com.github.h3yow.reliablemessagequeuejava.queue.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.queue.model.PartitionBlocker;
import com.github.h3yow.reliablemessagequeuejava.queue.model.PublishResult;
import com.github.h3yow.reliablemessagequeuejava.queue.model.TopicConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionBlockerQueryTest {

    private static final Logger log = LoggerFactory.getLogger(PartitionBlockerQueryTest.class);

    @Test
    void reportsNoBlockerWhenPartitionIsClear(@TempDir Path dir) {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 10_000, 10, 50, 3);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        PublishResult m1 = q.publishText("t", "k", null, "a");
        PublishResult m2 = q.publishText("t", "k", null, "b");

        // 消费前：队首是 m1，READY（可立即投递）
        PartitionBlocker before = q.getPartitionBlocker("g", "t", 0);
        assertTrue(before.blocked());
        assertEquals("READY", before.state());
        assertEquals(m1.messageId(), before.messageId());
        assertEquals(0L, before.offset());
        assertEquals(0, before.attempts());

        // 全部确认后：没有卡点
        for (Delivery d : q.poll("g", "t", 10)) {
            QueueTestSupport.logDelivery("ack", d);
            q.ack(d);
        }
        PartitionBlocker clear = q.getPartitionBlocker("g", "t", 0);
        assertFalse(clear.blocked());
        assertNull(clear.state());
        assertNull(clear.messageId());
        assertEquals(m2.offset(), q.committedOffset("g", "t", 0));
    }

    @Test
    void reportsBackoffBlockerWithNextVisibleTimeAndAttempts(@TempDir Path dir) throws Exception {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 10_000, 500, 5_000, 5);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        PublishResult m1 = q.publishText("t", "k", null, "a");

        Delivery d = q.poll("g", "t", 10).get(0);
        q.nack(d, "boom-1"); // 退避中

        PartitionBlocker blocker = q.getPartitionBlocker("g", "t", 0);
        assertTrue(blocker.blocked());
        assertEquals("BACKOFF", blocker.state());
        assertEquals(m1.messageId(), blocker.messageId());
        assertEquals(1, blocker.attempts());
        assertEquals("boom-1", blocker.lastError());
        assertTrue(blocker.nextDeliverableAt() > System.currentTimeMillis(),
                "下一次可投递时间应在未来");

        // 卡点未解决前，同分区新发布的后续消息 b 不能被跨过投递
        q.publishText("t", "k", null, "b");
        List<Delivery> duringBackoff = q.poll("g", "t", 10);
        assertTrue(duringBackoff.isEmpty(), "队首退避中，同分区后续消息不应被投递");

        // 退避结束后仍由同一队首阻塞，但状态变为 READY，且下一次投递 attempt=2
        Thread.sleep(550);
        PartitionBlocker ready = q.getPartitionBlocker("g", "t", 0);
        assertTrue(ready.blocked());
        assertEquals("READY", ready.state());
        assertEquals(1, ready.attempts(), "已失败次数仍为 1，下一次投递才是 attempt=2");

        Delivery retry = awaitPollOne(q, "g", "t", 1);
        assertEquals(m1.messageId(), retry.messageId());
        assertEquals(2, retry.attempt());
        q.ack(retry);
        // 队首解决后，后面的 b 才被放行
        Delivery after = awaitPollOne(q, "g", "t", 10);
        assertEquals("b", QueueTestSupport.text(after));
        q.ack(after);
        assertFalse(q.getPartitionBlocker("g", "t", 0).blocked());
    }

    @Test
    void reportsLeasedThenLeaseExpiredBlocker(@TempDir Path dir) throws Exception {
        // 租约 200ms，便于制造超时
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 200, 10, 50, 5);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        PublishResult m1 = q.publishText("t", "k", null, "a");
        q.publishText("t", "k", null, "b");

        // 只租出队首 m1（max=1），让 b 留在队首之后等待
        Delivery leased = q.poll("g", "t", 1).get(0);
        assertEquals(m1.messageId(), leased.messageId());
        PartitionBlocker active = q.getPartitionBlocker("g", "t", 0);
        assertEquals("LEASED", active.state());
        assertEquals(m1.messageId(), active.messageId());
        assertTrue(active.leaseExpiresAt() != null
                        && active.leaseExpiresAt() > System.currentTimeMillis(),
                "在途租约应带未来的到期时间");
        assertEquals(active.leaseExpiresAt(), active.nextDeliverableAt());

        // 租约未到期前，后续 b 不被跨过
        assertTrue(q.poll("g", "t", 10).isEmpty());

        // 等到租约已到期但<em>不</em>触发回收：查询应如实反映 LEASE_EXPIRED
        try {
            Thread.sleep(260);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        PartitionBlocker expired = q.getPartitionBlocker("g", "t", 0);
        assertEquals("LEASE_EXPIRED", expired.state());
        assertEquals(m1.messageId(), expired.messageId());

        // 一次 poll 触发回收并重新投递（新的租约），仍只取回队首
        Delivery redelivered = q.poll("g", "t", 1).get(0);
        assertEquals(m1.messageId(), redelivered.messageId());
        q.ack(redelivered);
        Delivery b = q.poll("g", "t", 10).get(0);
        assertEquals("b", QueueTestSupport.text(b));
    }

    @Test
    void blockerSurvivesRestartAsReadyWithoutSkippingFollowingMessages(@TempDir Path dir) {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 10_000, 10, 50, 3);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        PublishResult m1 = q.publishText("t", "k", null, "a");
        q.publishText("t", "k", null, "b");

        // m1 投递后不确认，模拟应用崩溃/重启
        Delivery leased = q.poll("g", "t", 10).get(0);
        assertEquals(m1.messageId(), leased.messageId());
        q.close();

        ReliableMessageQueue reopened = QueueTestSupport.reopen(dir, 10_000, 10, 50, 3);
        reopened.start(); // 旧租约立即失效

        // 重启后查询：仍是同一条队首，状态 READY（尚未重新投递），位点未丢
        PartitionBlocker blocker = reopened.getPartitionBlocker("g", "t", 0);
        assertTrue(blocker.blocked());
        assertEquals("READY", blocker.state());
        assertEquals(m1.messageId(), blocker.messageId());
        assertEquals(0L, blocker.offset());

        // 重新投递从队首 m1 开始，不会跳过它直接投 b
        List<Delivery> again = reopened.poll("g", "t", 10);
        assertEquals(2, again.size(), "m1、b 都未解决且可投递，按位点顺序一起返回");
        assertEquals(m1.messageId(), again.get(0).messageId());
        assertEquals("a", QueueTestSupport.text(again.get(0)));
        assertEquals("b", QueueTestSupport.text(again.get(1)));
        reopened.close();
    }

    @Test
    void deadLetteredHeadIsNotABlockerAndCommittedProgressIsReported(@TempDir Path dir) throws Exception {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 10_000, 10, 50, 1);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        PublishResult poison = q.publishText("t", "k", null, "poison");
        PublishResult next = q.publishText("t", "k", null, "next");

        Delivery d = q.poll("g", "t", 1).get(0);
        assertEquals(poison.messageId(), d.messageId());
        q.nack(d, "fatal"); // maxAttempts=1 -> 进死信，视为已解决

        // 死信不构成卡点：队首推进到 next（尚未消费）-> READY 卡点是 next
        PartitionBlocker blocker = q.getPartitionBlocker("g", "t", 0);
        assertTrue(blocker.blocked());
        assertEquals(next.messageId(), blocker.messageId());
        assertEquals(poison.offset(), q.committedOffset("g", "t", 0));

        q.ack(awaitPollOne(q, "g", "t", 10));
        assertFalse(q.getPartitionBlocker("g", "t", 0).blocked());
        log.info("死信队首已放行，分区无卡点 committed={}", q.committedOffset("g", "t", 0));
    }

    /** 在 3s 内反复 poll 直到出现至少一条（兼容退避时间边界）。 */
    private static Delivery awaitPollOne(ReliableMessageQueue q, String group, String topic, int max)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3_000;
        while (System.currentTimeMillis() < deadline) {
            List<Delivery> batch = q.poll(group, topic, max);
            if (!batch.isEmpty()) {
                return batch.get(0);
            }
            Thread.sleep(25);
        }
        throw new AssertionError("预期能 poll 到消息，但一直为空");
    }
}
