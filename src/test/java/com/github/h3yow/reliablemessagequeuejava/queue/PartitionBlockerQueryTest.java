package com.github.h3yow.reliablemessagequeuejava.queue;

import com.github.h3yow.reliablemessagequeuejava.queue.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.queue.model.PartitionBlocker;
import com.github.h3yow.reliablemessagequeuejava.queue.model.PartitionBlockerReport;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    // ---------------- 只给主题+分区（消费组可省略）的聚合查询 ----------------

    @Test
    void topicPartitionOnlyQueryReportsNoBlockerAndDoesNotRequireGroup(@TempDir Path dir) {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 10_000, 10, 50, 3);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        q.publishText("t", "k", null, "a");
        q.publishText("t", "k", null, "b");

        // 还没有任何消费组建立消费：只给主题+分区也能查，明确返回没有卡点
        PartitionBlockerReport beforeAnyGroup = q.getPartitionBlockers(null, "t", 0);
        assertFalse(beforeAnyGroup.blocked());
        assertTrue(beforeAnyGroup.blockers().isEmpty());

        // 两个消费组分别把消息全部确认 -> 该分区对所有组都没有卡点
        for (String g : List.of("g1", "g2")) {
            for (Delivery d : q.poll(g, "t", 10)) {
                q.ack(d);
            }
        }
        PartitionBlockerReport clear = q.getPartitionBlockers(null, "t", 0);
        assertFalse(clear.blocked(), "所有消费组都没有卡点时必须明确返回没有卡点");
        assertTrue(clear.blockers().isEmpty());

        // 分区越界仍按非法参数拒绝
        assertThrows(com.github.h3yow.reliablemessagequeuejava.queue.exception.InvalidTopicException.class,
                () -> q.getPartitionBlockers(null, "t", 7));
    }

    @Test
    void topicPartitionOnlyQueryShowsEachGroupBlockerSeparately(@TempDir Path dir) throws Exception {
        // 租约 200ms，便于同时制造在途与超时两种状态
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 200, 500, 5_000, 5);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        PublishResult g1Msg = q.publishText("t", "k", null, "g1-poison");
        q.publishText("t", "k", null, "g2-poison");

        // g1：只投递队首并保持不确认 -> 在途租约（g1 只持有 offset0）
        Delivery g1Leased = q.poll("g1", "t", 1).get(0);
        assertEquals(g1Msg.messageId(), g1Leased.messageId());
        // g2：同样只投递队首（max=1），拿到的是它自己视角下的 offset0，然后 nack -> 退避中
        Delivery g2Nacked = q.poll("g2", "t", 1).get(0);
        assertEquals(g1Msg.messageId(), g2Nacked.messageId());
        q.nack(g2Nacked, "g2-boom");

        PartitionBlockerReport report = q.getPartitionBlockers(null, "t", 0);
        assertTrue(report.blocked());
        // 不同消费组各自的卡点都能分别看到，按组名升序
        assertEquals(List.of("g1", "g2"),
                report.blockers().stream().map(PartitionBlocker::group).toList());
        PartitionBlocker g1Blocker = report.blockers().get(0);
        PartitionBlocker g2Blocker = report.blockers().get(1);
        assertEquals("LEASED", g1Blocker.state());
        assertEquals(g1Msg.messageId(), g1Blocker.messageId());
        assertTrue(g1Blocker.leaseExpiresAt() != null
                && g1Blocker.leaseExpiresAt() > System.currentTimeMillis());
        assertEquals("BACKOFF", g2Blocker.state());
        assertEquals(g1Msg.messageId(), g2Blocker.messageId());
        assertEquals("g2-boom", g2Blocker.lastError());
        assertEquals(1, g2Blocker.attempts());
        assertTrue(g2Blocker.nextDeliverableAt() > System.currentTimeMillis());

        // 指定消费组查询等价于聚合视图里的对应那一条
        PartitionBlocker onlyG2 = q.getPartitionBlockers("g2", "t", 0).blockers().get(0);
        assertEquals("BACKOFF", onlyG2.state());
        assertEquals(g1Msg.messageId(), onlyG2.messageId());

        // 等 g1 租约超时但不触发回收：聚合视图里 g1 变为 LEASE_EXPIRED，g2 仍是 BACKOFF
        Thread.sleep(260);
        PartitionBlockerReport afterExpiry = q.getPartitionBlockers(null, "t", 0);
        assertTrue(afterExpiry.blocked());
        java.util.Map<String, String> states = new java.util.HashMap<>();
        for (PartitionBlocker b : afterExpiry.blockers()) {
            states.put(b.group(), b.state());
        }
        assertEquals("LEASE_EXPIRED", states.get("g1"));
        assertEquals("BACKOFF", states.get("g2"));

        // 卡点解决前后面的消息不会被跨过：
        //  - g1 租约已超时，poll 触发回收后会重新从队首（g1-poison）开始按位点投递，队首一定是它；
        //  - g2 退避未结束（500ms），队首不可见，poll 为空。
        q.publishText("t", "k", null, "trailing");
        List<Delivery> g1Redelivered = q.poll("g1", "t", 10);
        assertFalse(g1Redelivered.isEmpty());
        assertEquals(g1Msg.messageId(), g1Redelivered.get(0).messageId(),
                "租约超时重新投递必须仍从队首开始");
        assertEquals(0L, g1Redelivered.get(0).offset());
        assertTrue(q.poll("g2", "t", 10).isEmpty(), "g2 退避未结束，队首不可见且不能跨过它投 trailing");
    }

    @Test
    void topicPartitionOnlyQueryReflectsRestartReadyAndClearAfterResolution(@TempDir Path dir) {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 10_000, 10, 50, 3);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        PublishResult g1Msg = q.publishText("t", "k", null, "g1");
        q.publishText("t", "k", null, "g2");

        // 两个组都投递但不确认，模拟应用崩溃
        Delivery g1Leased = q.poll("g1", "t", 1).get(0);
        Delivery g2Leased = q.poll("g2", "t", 1).get(0);
        assertEquals(g1Msg.messageId(), g1Leased.messageId());
        q.close();

        ReliableMessageQueue reopened = QueueTestSupport.reopen(dir, 10_000, 10, 50, 3);
        reopened.start(); // 旧租约立即失效

        // 重启后只给主题+分区：两个组都卡在各自队首，状态 READY（尚未重新投递）
        PartitionBlockerReport afterRestart = reopened.getPartitionBlockers(null, "t", 0);
        assertTrue(afterRestart.blocked());
        assertEquals(2, afterRestart.blockers().size());
        for (PartitionBlocker b : afterRestart.blockers()) {
            assertEquals("READY", b.state(), "group=" + b.group());
            assertTrue(b.nextDeliverableAt() <= System.currentTimeMillis());
            assertNull(b.leaseExpiresAt());
        }

        // 两组各自重新投递都从队首开始（不跨过），确认后聚合视图转为无卡点
        for (String g : List.of("g1", "g2")) {
            List<Delivery> ds = reopened.poll(g, "t", 10);
            assertEquals(2, ds.size());
            ds.forEach(reopened::ack);
        }
        PartitionBlockerReport clear = reopened.getPartitionBlockers(null, "t", 0);
        assertFalse(clear.blocked());
        assertTrue(clear.blockers().isEmpty());
        reopened.close();
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
