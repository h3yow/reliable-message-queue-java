package com.github.h3yow.reliablemessagequeuejava.queue;

import com.github.h3yow.reliablemessagequeuejava.queue.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.queue.model.PartitionBlocker;
import com.github.h3yow.reliablemessagequeuejava.queue.model.PartitionBlockerView;
import com.github.h3yow.reliablemessagequeuejava.queue.model.PublishResult;
import com.github.h3yow.reliablemessagequeuejava.queue.model.TopicConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionBlockerQueryTest {

    private static int partitionOf(PublishResult r) {
        return r.partition();
    }

    @Test
    void reportsNoBlockerWhenPartitionIsFullyResolved(@TempDir Path dir) {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 30_000, 10, 100, 3);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        PublishResult m = q.publishText("t", "k", null, "a");
        List<Delivery> batch = q.poll("g", "t", 10);
        q.ack(batch.get(0));

        PartitionBlockerView view = q.getPartitionBlocker("t", m.partition());
        assertFalse(view.blocked());
        assertTrue(view.blockers().isEmpty());
    }

    @Test
    void reportsLeasedThenDeliverableAndPreventsJumpingAhead(@TempDir Path dir) {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 30_000, 10, 100, 3);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        PublishResult first = q.publishText("t", "k", null, "first");
        q.publishText("t", "k", null, "second");

        // 只投出队头 first 并保持未确认（租约中）
        List<Delivery> batch = q.poll("g", "t", 10);
        assertEquals(2, batch.size());
        // 租约未到期前：卡点为 first，状态 LEASED
        PartitionBlockerView leased = q.getPartitionBlocker("t", first.partition());
        assertTrue(leased.blocked());
        assertEquals(1, leased.blockers().size());
        PartitionBlocker b = leased.blockers().get(0);
        assertEquals("g", b.group());
        assertEquals(first.messageId(), b.messageId());
        assertEquals(first.offset(), b.offset());
        assertEquals(PartitionBlocker.LEASED, b.state());
        assertEquals(0, b.attempts());
        assertTrue(b.nextDeliverAt() > System.currentTimeMillis() - 1_000L);
    }

    @Test
    void reportsBackoffWithAttemptsAndNextVisibleTime(@TempDir Path dir) throws Exception {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 30_000, 500, 5_000, 3);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        PublishResult first = q.publishText("t", "k", null, "first");
        q.publishText("t", "k", null, "second");

        List<Delivery> batch = q.poll("g", "t", 10);
        Delivery head = batch.stream().filter(d -> d.messageId().equals(first.messageId()))
                .findFirst().orElseThrow();
        q.nack(head, "boom-1");

        // 退避期内：卡点为 first，状态 BACKOFF，attempts=1，且后面的 second 不能跨过它投递
        PartitionBlockerView backoff = q.getPartitionBlocker("t", first.partition());
        assertTrue(backoff.blocked());
        PartitionBlocker b = backoff.blockers().get(0);
        assertEquals(first.messageId(), b.messageId());
        assertEquals(PartitionBlocker.BACKOFF, b.state());
        assertEquals(1, b.attempts());
        assertEquals("boom-1", b.lastError());
        assertTrue(b.nextDeliverAt() > System.currentTimeMillis());
        assertTrue(q.poll("g", "t", 10).stream().noneMatch(d -> d.offset() > first.offset()),
                "退避中的队头不得被后面的消息跨过");

        // 退避结束：first 再次可投递，卡点状态变为 DELIVERABLE
        Thread.sleep(600);
        PartitionBlockerView ready = q.getPartitionBlocker("t", first.partition());
        assertEquals(PartitionBlocker.DELIVERABLE, ready.blockers().get(0).state());
    }

    @Test
    void reportsBlockerAfterRestartAsDeliverable(@TempDir Path dir) throws Exception {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 60_000, 10, 100, 3);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        PublishResult first = q.publishText("t", "k", null, "first");
        q.publishText("t", "k", null, "second");
        List<Delivery> batch = q.poll("g", "t", 10);
        assertEquals(2, batch.size());
        // 未确认就关闭：旧租约随持久化保留
        q.close();

        // 重启前直接 new 出的实例（未调用 start）仍能反映真实状态
        ReliableMessageQueue reopened = QueueTestSupport.reopen(dir, 60_000, 10, 100, 3);
        PartitionBlockerView beforeStart = reopened.getPartitionBlocker("t", first.partition());
        assertTrue(beforeStart.blocked());
        assertEquals(first.messageId(), beforeStart.blockers().get(0).messageId());

        // 调用 start 模拟应用重启：旧租约立即失效，队头变为 DELIVERABLE，且不会跨过它投递 second
        reopened.start();
        PartitionBlockerView afterStart = reopened.getPartitionBlocker("t", first.partition());
        assertTrue(afterStart.blocked());
        PartitionBlocker b = afterStart.blockers().get(0);
        assertEquals(first.messageId(), b.messageId());
        assertEquals(PartitionBlocker.DELIVERABLE, b.state());

        List<Delivery> redelivered = reopened.poll("g", "t", 10);
        assertEquals(2, redelivered.size(), "重启后两条未确认消息都可重新投递");
        assertEquals(first.offset(), redelivered.get(0).offset(), "队头仍然排在最前");
        reopened.close();
    }

    @Test
    void reportsPerGroupBlockersIndependently(@TempDir Path dir) {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 30_000, 10, 100, 3);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        PublishResult m = q.publishText("t", "k", null, "a");
        // g1 未确认（有卡点）；g2 直接确认（无卡点）
        q.poll("g1", "t", 10);
        List<Delivery> g2 = q.poll("g2", "t", 10);
        g2.forEach(q::ack);

        PartitionBlockerView view = q.getPartitionBlocker("t", m.partition());
        assertTrue(view.blocked());
        assertEquals(1, view.blockers().size());
        assertEquals("g1", view.blockers().get(0).group());
        assertEquals(m.messageId(), view.blockers().get(0).messageId());
    }
}
