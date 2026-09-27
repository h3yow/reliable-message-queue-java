package com.github.h3yow.reliablemessagequeuejava.queue;

import com.github.h3yow.reliablemessagequeuejava.queue.exception.BatchReplayRejectedException;
import com.github.h3yow.reliablemessagequeuejava.queue.model.BatchReplayResult;
import com.github.h3yow.reliablemessagequeuejava.queue.model.DeadLetter;
import com.github.h3yow.reliablemessagequeuejava.queue.model.DeadLetterFilter;
import com.github.h3yow.reliablemessagequeuejava.queue.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.queue.model.PublishResult;
import com.github.h3yow.reliablemessagequeuejava.queue.model.ReplayResult;
import com.github.h3yow.reliablemessagequeuejava.queue.model.TopicConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchDeadLetterReplayTest {

    /** 发布 count 条消息并全部 nack 到进死信（maxAttempts=1），按发布顺序返回死信记录。 */
    private static List<DeadLetter> poison(ReliableMessageQueue q, String topic, String key,
                                           int count) {
        for (int i = 0; i < count; i++) {
            q.publishText(topic, key, null, "p" + i);
        }
        while (true) {
            List<Delivery> batch = q.poll("g", topic, 100);
            if (batch.isEmpty()) {
                break;
            }
            for (Delivery d : batch) {
                QueueTestSupport.logDelivery("nack", d);
                q.nack(d, "boom");
            }
        }
        List<DeadLetter> dead = new ArrayList<>(q.listDeadLetters("g", topic));
        dead.sort((a, b) -> Long.compare(a.originalOffset(), b.originalOffset()));
        return dead;
    }

    @Test
    void batchReplaySucceedsAndReentersDeliveryInOrder(@TempDir Path dir) {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 10_000, 10, 50, 1);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        List<DeadLetter> dead = poison(q, "t", "k", 3);
        assertEquals(3, dead.size());

        BatchReplayResult result = q.replayDeadLetters(DeadLetterFilter.forTopic("t"));
        assertEquals(3, result.requested());
        assertEquals(3, result.replayed());

        // 每条死信都标记为已重放，且新位点严格按原分区先后顺序连续分配
        List<ReplayResult> rs = result.results();
        for (int i = 0; i < 3; i++) {
            assertEquals("p" + i, QueueTestSupport.textPayload(rs.get(i).deadLetter().payload()));
            assertTrue(rs.get(i).deadLetter().replayed());
            assertEquals(3L + i, rs.get(i).publishResult().offset());
            assertEquals(dead.get(i).deadLetterId(), rs.get(i).deadLetter().deadLetterId());
        }

        // 重放消息走正常投递，且同分区按原顺序被消费
        List<Delivery> consumed = new ArrayList<>();
        while (true) {
            List<Delivery> batch = q.poll("g", "t", 100);
            if (batch.isEmpty()) {
                break;
            }
            consumed.addAll(batch);
            for (Delivery d : batch) {
                q.ack(d);
            }
        }
        assertEquals(3, consumed.size());
        for (int i = 0; i < 3; i++) {
            assertEquals("p" + i, QueueTestSupport.text(consumed.get(i)));
            assertEquals(3L + i, consumed.get(i).offset());
        }
    }

    @Test
    void oneMessageThatCannotFitRejectsWholeBatchAtomically(@TempDir Path dir) {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 10_000, 10, 50, 1);
        // 容量口径：死信视为已解决、释放容量。容量 4：2 条死信 + 4 条普通未消费消息占满，
        // 重放批次 2 条 -> pending(4)+2=6 > 4，整批进不去。
        q.createTopic(new TopicConfig("t", 1, 4, 1 << 20));
        poison(q, "t", "k", 2);
        for (int i = 0; i < 4; i++) {
            q.publishText("t", "k", null, "occupy" + i);
        }

        BatchReplayRejectedException ex = assertThrows(BatchReplayRejectedException.class,
                () -> q.replayDeadLetters(DeadLetterFilter.forTopic("t")));
        assertTrue(ex.getMessage().contains("容量"));

        // 整批回滚：没有任何新消息、没有任何死信被标记、没有产生 replay 幂等键
        assertEquals(2, q.listDeadLetters("g", "t").size());
        for (DeadLetter d : q.listDeadLetters("g", "t")) {
            assertFalse(d.replayed(), "被拒绝批次不得改动任何死信: " + d.deadLetterId());
        }
        // 拒绝后原有未消费消息仍可正常投递，死信消息不会复活
        List<Delivery> stillDelivered = q.poll("g", "t", 100);
        assertEquals(4, stillDelivered.size());
        for (Delivery d : stillDelivered) {
            assertTrue(QueueTestSupport.text(d).startsWith("occupy"));
        }
    }

    @Test
    void rejectedBatchCanBeResubmittedAsIsAfterFreeingCapacity(@TempDir Path dir) {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 10_000, 10, 50, 1);
        q.createTopic(new TopicConfig("t", 1, 4, 1 << 20));
        poison(q, "t", "k", 2);
        // 占满容量（4 条未解决普通消息），重放必被整批拒绝
        for (int i = 0; i < 4; i++) {
            q.publishText("t", "k", null, "hold" + i);
        }
        List<Delivery> holders = q.poll("g", "t", 100);
        assertEquals(4, holders.size());

        DeadLetterFilter filter = DeadLetterFilter.forTopic("t");
        assertThrows(BatchReplayRejectedException.class, () -> q.replayDeadLetters(filter));

        // 腾出容量（确认占位消息）后，用同一个 filter 原样再提交，必须成功
        for (Delivery d : holders) {
            q.ack(d);
        }
        BatchReplayResult result = q.replayDeadLetters(filter);
        assertEquals(2, result.replayed());
    }

    @Test
    void replayKeepsCommittedProgressAndPartitionOrder(@TempDir Path dir) {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 10_000, 10, 50, 1);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        // 一条正常消息先确认，再让 3 条消息进死信
        PublishResult ok = q.publishText("t", "k", null, "ok");
        List<Delivery> first = q.poll("g", "t", 100);
        assertEquals(1, first.size());
        q.ack(first.get(0));
        long committedAfterOk = q.committedOffset("g", "t", 0);
        assertEquals(ok.offset(), committedAfterOk);

        List<DeadLetter> dead = poison(q, "t", "k", 3);
        long committedBeforeReplay = q.committedOffset("g", "t", 0);
        // 死信视为已解决，位点连续推进到死信最后一条
        assertEquals(dead.get(2).originalOffset(), committedBeforeReplay);

        BatchReplayResult result = q.replayDeadLetters(DeadLetterFilter.forTopic("t"));
        // 已有已确认进度不被改动
        assertEquals(committedBeforeReplay, q.committedOffset("g", "t", 0));

        // 新消息位点都在旧位点之后，顺序与原死信顺序一致
        for (int i = 0; i < 3; i++) {
            assertTrue(result.results().get(i).publishResult().offset() > committedBeforeReplay);
        }
        List<Delivery> consumed = new ArrayList<>();
        while (true) {
            List<Delivery> batch = q.poll("g", "t", 100);
            if (batch.isEmpty()) {
                break;
            }
            consumed.addAll(batch);
            batch.forEach(q::ack);
        }
        assertEquals(List.of("p0", "p1", "p2"),
                consumed.stream().map(QueueTestSupport::text).toList());
    }

    @Test
    void sameFilterRepeatedIsDeterministicAndAlreadyReplayedRejects(@TempDir Path dir) {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 10_000, 10, 50, 1);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        poison(q, "t", "k", 3);

        DeadLetterFilter filter = DeadLetterFilter.forTopic("t");
        BatchReplayResult first = q.replayDeadLetters(filter);
        // 再次用同样条件提交：命中的死信都已重放，整批拒绝（不会产生重复副本）
        assertThrows(BatchReplayRejectedException.class, () -> q.replayDeadLetters(filter));

        // 顺序可复现：结果顺序与按（分区、原位点）排序一致
        List<Long> offsets = first.results().stream()
                .map(r -> r.deadLetter().originalOffset()).toList();
        List<Long> sorted = new ArrayList<>(offsets);
        sorted.sort(Long::compare);
        assertEquals(sorted, offsets);
    }

    @Test
    void filtersByGroupPartitionAndDeadAtRange(@TempDir Path dir) throws Exception {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 10_000, 10, 50, 1);
        // 单分区：分区过滤恒命中，主要验证 group 与时间范围；分区维度用越界/不命中场景补充
        q.createTopic(new TopicConfig("t", 1, 1000, 1 << 20));

        // gA 的死信（较早）
        q.publishText("t", "k", null, "a");
        Delivery da = q.poll("gA", "t", 100).get(0);
        q.nack(da, "boom");
        DeadLetter dlA = q.listDeadLetters("gA", "t").get(0);

        Thread.sleep(20);
        long boundary = System.currentTimeMillis();
        Thread.sleep(20);

        // gB 的死信（较晚）
        q.publishText("t", "k", null, "b");
        Delivery db = q.poll("gB", "t", 100).get(0);
        q.nack(db, "boom");
        DeadLetter dlB = q.listDeadLetters("gB", "t").get(0);

        assertTrue(dlA.deadAt() < boundary && dlB.deadAt() > boundary);

        // 按消费组过滤：只重放 gA
        BatchReplayResult byGroup = q.replayDeadLetters(
                new DeadLetterFilter("t", "gA", null, null, null));
        assertEquals(1, byGroup.replayed());
        assertEquals(dlA.deadLetterId(), byGroup.results().get(0).deadLetter().deadLetterId());

        // gA 已重放：不带 group 再提会因 gA 已重放而整批拒绝
        assertThrows(BatchReplayRejectedException.class,
                () -> q.replayDeadLetters(DeadLetterFilter.forTopic("t")));

        // 按时间范围只圈定较晚的 gB（排除已重放的 gA），可成功重放 gB
        BatchReplayResult byTime = q.replayDeadLetters(
                new DeadLetterFilter("t", null, null, boundary, null));
        assertEquals(1, byTime.replayed());
        assertEquals(dlB.deadLetterId(), byTime.results().get(0).deadLetter().deadLetterId());

        // 不命中任何记录的时间窗 -> 空批次（无操作，不报错）
        BatchReplayResult emptyWindow = q.replayDeadLetters(
                new DeadLetterFilter("t", null, null, 1L, 2L));
        assertEquals(0, emptyWindow.requested());
    }

    @Test
    void invalidPartitionOrTimeRangeIsRejected(@TempDir Path dir) {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 10_000, 10, 50, 1);
        q.createTopic(new TopicConfig("t", 1, 1000, 1 << 20));
        assertThrows(com.github.h3yow.reliablemessagequeuejava.queue.exception.InvalidTopicException.class,
                () -> q.replayDeadLetters(new DeadLetterFilter("t", null, 5, null, null)));
        assertThrows(com.github.h3yow.reliablemessagequeuejava.queue.exception.InvalidTopicException.class,
                () -> q.replayDeadLetters(new DeadLetterFilter("t", null, null, 10L, 5L)));
    }

    @Test
    void crossPartitionBatchOrdersByPartitionThenOriginalOffset(@TempDir Path dir) throws Exception {
        int partitionCount = 4;
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 10_000, 10, 50, 1);
        q.createTopic(new TopicConfig("t", partitionCount, 1000, 1 << 20));

        // 预先挑出两个不同分区，每个分区各 2 个确定的分区键（String.hashCode 确定）
        java.util.Map<Integer, List<String>> keysByPartition = new java.util.TreeMap<>();
        for (int i = 0; i < 100_000 && !twoPartitionsWithTwoKeys(keysByPartition); i++) {
            String key = "key-" + i;
            int p = Math.floorMod(key.hashCode(), partitionCount);
            if (!keysByPartition.containsKey(p) && keysByPartition.size() >= 2) {
                continue; // 已锁定前两个分区，忽略其余分区
            }
            List<String> list = keysByPartition.computeIfAbsent(p, k -> new ArrayList<>());
            if (list.size() < 2) {
                list.add(key);
            }
        }
        assertTrue(twoPartitionsWithTwoKeys(keysByPartition), "应能构造两个不同分区各 2 个键");
        for (List<String> v : keysByPartition.values()) {
            assertEquals(2, v.size());
        }

        java.util.Map<Integer, List<String>> expectedByPartition = new java.util.TreeMap<>();
        // 按分区、分区键顺序发布并各 nack 一次进死信
        for (var e : keysByPartition.entrySet()) {
            for (String key : e.getValue()) {
                PublishResult pm = q.publishText("t", key, null, "pay-" + key);
                Delivery d = q.poll("g", "t", 100).stream()
                        .filter(x -> x.messageId().equals(pm.messageId())).findFirst().orElseThrow();
                q.nack(d, "boom");
                expectedByPartition.computeIfAbsent(e.getKey(), k -> new ArrayList<>())
                        .add("pay-" + key);
                Thread.sleep(2);
            }
        }

        BatchReplayResult result = q.replayDeadLetters(DeadLetterFilter.forTopic("t"));
        assertEquals(4, result.replayed());

        // 结果必须按 (partition, originalOffset) 升序，可复现
        int prevPartition = -1;
        long prevOffset = -1;
        for (ReplayResult r : result.results()) {
            DeadLetter dl = r.deadLetter();
            if (dl.partition() != prevPartition) {
                assertTrue(dl.partition() > prevPartition, "分区必须升序");
                prevPartition = dl.partition();
                prevOffset = -1;
            }
            assertTrue(dl.originalOffset() > prevOffset, "同分区原位点必须升序");
            prevOffset = dl.originalOffset();
        }

        // 每个分区内重放副本按原顺序消费
        java.util.Map<Integer, List<Delivery>> consumedByPartition = new java.util.TreeMap<>();
        int totalConsumed = 0;
        int emptyRounds = 0;
        while (totalConsumed < 4 && emptyRounds < 10) {
            List<Delivery> batch = q.poll("g", "t", 100);
            if (batch.isEmpty()) {
                emptyRounds++;
                Thread.sleep(5);
                continue;
            }
            emptyRounds = 0;
            for (Delivery d : batch) {
                consumedByPartition.computeIfAbsent(d.partition(), k -> new ArrayList<>()).add(d);
                totalConsumed++;
                q.ack(d);
            }
        }
        assertEquals(4, totalConsumed);
        for (var e : expectedByPartition.entrySet()) {
            int partition = e.getKey();
            List<String> payloads = consumedByPartition
                    .getOrDefault(partition, new ArrayList<>()).stream()
                    .map(QueueTestSupport::text).toList();
            assertEquals(e.getValue(), payloads, "分区 " + partition + " 重放消费顺序被破坏");
        }
    }

    private static boolean twoPartitionsWithTwoKeys(java.util.Map<Integer, List<String>> map) {
        return map.size() == 2 && map.values().stream().allMatch(l -> l.size() == 2);
    }
}

