package com.github.h3yow.reliablemessagequeuejava.queue;

import com.github.h3yow.reliablemessagequeuejava.queue.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.queue.model.PartitionBlocker;
import com.github.h3yow.reliablemessagequeuejava.queue.model.PartitionBlockers;
import com.github.h3yow.reliablemessagequeuejava.queue.model.PublishResult;
import com.github.h3yow.reliablemessagequeuejava.queue.model.TopicConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 只给主题和分区（不传消费组）时的分区卡点查询：
 * 无阻塞明确返回空、不同消费组分别可见，以及退避/在途/租约超时/重启场景。
 */
class PartitionBlockerQueryWithoutGroupTest {

    private static ReliableMessageQueue queue(Path dir, long leaseMillis, int maxAttempts) {
        return QueueTestSupport.newQueue(dir, leaseMillis, 500, 5_000, maxAttempts);
    }

    private static Map<String, PartitionBlocker> byGroup(PartitionBlockers blockers) {
        return blockers.groups().stream()
                .collect(Collectors.toMap(PartitionBlocker::group, Function.identity()));
    }

    @Test
    void noGroupParameterReportsNoBlockerWhenPartitionIsClear(@TempDir Path dir) {
        ReliableMessageQueue q = queue(dir, 10_000, 3);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        q.publishText("t", "k", null, "a");

        // 尚无消费组：没有卡点，明确返回 blocked=false、groups=[]
        PartitionBlockers beforeAnyGroup = q.getPartitionBlockers("t", 0);
        assertFalse(beforeAnyGroup.blocked());
        assertTrue(beforeAnyGroup.groups().isEmpty());

        // g1 消费确认后：仍没有卡点
        for (Delivery d : q.poll("g1", "t", 10)) {
            q.ack(d);
        }
        PartitionBlockers clear = q.getPartitionBlockers("t", 0);
        assertFalse(clear.blocked());
        assertTrue(clear.groups().isEmpty());
    }

    @Test
    void differentGroupsBlockedIndependentlyAndEachIsReported(@TempDir Path dir) {
        ReliableMessageQueue q = queue(dir, 10_000, 5);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        PublishResult m = q.publishText("t", "k", null, "a");

        // 两个组都先投递：g1 随后确认（无阻塞），g2 保持在途租约（LEASED 卡点）
        q.poll("g1", "t", 10).forEach(q::ack);
        Delivery g2Leased = q.poll("g2", "t", 1).get(0);
        assertEquals(m.messageId(), g2Leased.messageId());

        PartitionBlockers blockers = q.getPartitionBlockers("t", 0);
        assertTrue(blockers.blocked());
        assertEquals(1, blockers.groups().size());
        PartitionBlocker g2 = byGroup(blockers).get("g2");
        assertEquals("LEASED", g2.state());
        assertEquals(m.messageId(), g2.messageId());
        // 已确认的 g1 不出现在卡点列表里
        assertNull(byGroup(blockers).get("g1"));

        // g2 也确认后：两个组都没有卡点
        q.ack(g2Leased);
        assertFalse(q.getPartitionBlockers("t", 0).blocked());
    }

    @Test
    void backoffAndLeasedStatesAreReportedPerGroup(@TempDir Path dir) {
        ReliableMessageQueue q = queue(dir, 10_000, 5);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        q.publishText("t", "k", null, "a");

        // g1：投递后 nack -> 退避中；g2：只投递不确认 -> 在途租约
        Delivery g1d = q.poll("g1", "t", 1).get(0);
        q.nack(g1d, "g1-boom");
        Delivery g2d = q.poll("g2", "t", 1).get(0);
        assertEquals(g1d.messageId(), g2d.messageId());

        PartitionBlockers blockers = q.getPartitionBlockers("t", 0);
        Map<String, PartitionBlocker> map = byGroup(blockers);
        assertEquals(2, blockers.groups().size());
        assertEquals("BACKOFF", map.get("g1").state());
        assertEquals(1, map.get("g1").attempts());
        assertEquals("g1-boom", map.get("g1").lastError());
        assertTrue(map.get("g1").nextDeliverableAt() > System.currentTimeMillis());
        assertEquals("LEASED", map.get("g2").state());
        assertTrue(map.get("g2").leaseExpiresAt() > System.currentTimeMillis());

        // 卡点未解决前，两个组都不能跨过队首：再次 poll 均为空
        assertTrue(q.poll("g1", "t", 10).isEmpty());
        assertTrue(q.poll("g2", "t", 10).isEmpty());
    }

    @Test
    void leaseExpiredStateIsReportedWithoutReaping(@TempDir Path dir) throws Exception {
        // 租约 200ms：等到超时但不触发回收，查询要如实反映 LEASE_EXPIRED
        ReliableMessageQueue q = queue(dir, 200, 5);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        PublishResult m = q.publishText("t", "k", null, "a");
        Delivery leased = q.poll("g1", "t", 1).get(0);
        assertEquals(m.messageId(), leased.messageId());
        Thread.sleep(260);

        PartitionBlockers blockers = q.getPartitionBlockers("t", 0);
        Map<String, PartitionBlocker> map = byGroup(blockers);
        assertEquals("LEASE_EXPIRED", map.get("g1").state());
        assertEquals(m.messageId(), map.get("g1").messageId());
        assertTrue(map.get("g1").leaseExpiresAt() <= System.currentTimeMillis());
        // 查询是只读的：过期租约仍在，poll 之前状态未被回收
        assertEquals("LEASE_EXPIRED", q.getPartitionBlockers("t", 0).groups().get(0).state());
    }

    @Test
    void readyStateAfterRestartIsReportedForEveryGroup(@TempDir Path dir) {
        ReliableMessageQueue q = queue(dir, 10_000, 3);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        PublishResult m = q.publishText("t", "k", null, "a");
        // 两个组都投递了但都没确认 -> 重启后各自应看到 READY 卡点
        q.poll("g1", "t", 1);
        q.poll("g2", "t", 1);
        q.close();

        ReliableMessageQueue reopened = queue(dir, 10_000, 3);
        reopened.start(); // 旧租约立即失效
        PartitionBlockers blockers = reopened.getPartitionBlockers("t", 0);
        Map<String, PartitionBlocker> map = byGroup(blockers);
        assertEquals(2, blockers.groups().size());
        for (String g : List.of("g1", "g2")) {
            assertTrue(map.get(g).blocked());
            assertEquals("READY", map.get(g).state());
            assertEquals(m.messageId(), map.get(g).messageId());
            assertEquals(0L, map.get(g).offset());
        }

        // 重启后重新投递从队首开始，两组都拿到同一条，不会跳过
        List<Delivery> g1again = reopened.poll("g1", "t", 10);
        List<Delivery> g2again = reopened.poll("g2", "t", 10);
        assertEquals(List.of(m.messageId()), g1again.stream().map(Delivery::messageId).toList());
        assertEquals(List.of(m.messageId()), g2again.stream().map(Delivery::messageId).toList());
        reopened.close();
    }
}
