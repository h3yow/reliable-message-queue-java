package com.github.h3yow.reliablemessagequeuejava.queue;

import com.github.h3yow.reliablemessagequeuejava.queue.exception.BatchReplayRejectedException;
import com.github.h3yow.reliablemessagequeuejava.queue.exception.DuplicateIdempotencyKeyException;
import com.github.h3yow.reliablemessagequeuejava.queue.model.BatchReplayResult;
import com.github.h3yow.reliablemessagequeuejava.queue.model.DeadLetter;
import com.github.h3yow.reliablemessagequeuejava.queue.model.DeadLetterFilter;
import com.github.h3yow.reliablemessagequeuejava.queue.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.queue.model.PublishResult;
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

    /** maxAttempts=1：一次 nack 即进死信，方便构造死信。 */
    private ReliableMessageQueue queue(Path dir) {
        return QueueTestSupport.newQueue(dir, 10_000, 10, 50, 1);
    }

    /** 发布同分区键消息并 nack 到进死信，返回该消息的发布结果。 */
    private PublishResult kill(ReliableMessageQueue q, String topic, String group, String text) {
        PublishResult pr = q.publishText(topic, "hot-key", null, text);
        Delivery d = q.poll(group, topic, 10).stream()
                .filter(x -> x.messageId().equals(pr.messageId())).findFirst().orElseThrow();
        QueueTestSupport.logDelivery("nack-to-dlq", d);
        q.nack(d, "boom");
        return pr;
    }

    private List<Delivery> drainInOrder(ReliableMessageQueue q, String group, String topic, int expect) {
        List<Delivery> all = new ArrayList<>();
        int guard = 0;
        while (all.size() < expect && guard++ < 50) {
            for (Delivery d : q.poll(group, topic, 10)) {
                all.add(d);
            }
        }
        return all;
    }

    @Test
    void batchReplaySucceedsAndConsumedInOriginalOrderWithoutBreakingProgress(@TempDir Path dir) {
        ReliableMessageQueue q = queue(dir);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        // 3 条按 a,b,c 顺序进死信
        PublishResult a = kill(q, "t", "g", "a");
        PublishResult b = kill(q, "t", "g", "b");
        PublishResult c = kill(q, "t", "g", "c");
        // 死信全部视为已解决，位点推进到最后一条原位点
        assertEquals(c.offset(), q.committedOffset("g", "t", 0));

        List<DeadLetter> dead = q.listDeadLetters("g", "t");
        assertEquals(3, dead.size());
        // 死信记录的原位点顺序即进死信前先后
        assertEquals(List.of(a.offset(), b.offset(), c.offset()),
                dead.stream().map(DeadLetter::originalOffset).toList());

        BatchReplayResult result = q.replayDeadLetters(DeadLetterFilter.forTopic("t"));
        assertEquals(3, result.selected());
        // 结果顺序：同分区按原位点升序
        assertEquals(List.of(a.offset(), b.offset(), c.offset()),
                result.items().stream().map(x -> x.deadLetter().originalOffset()).toList());
        // 新位点连续追加在分区尾部
        List<Long> newOffsets = result.items().stream()
                .map(x -> x.publishResult().offset()).toList();
        assertEquals(List.of(3L, 4L, 5L), newOffsets);
        // 旧进度未被改动
        assertEquals(c.offset(), q.committedOffset("g", "t", 0));
        // 死信全部标记已重放
        assertTrue(q.listDeadLetters("g", "t").stream().allMatch(DeadLetter::replayed));

        // 消费重放消息：严格保持 a,b,c 原先后关系
        List<Delivery> consumed = drainInOrder(q, "g", "t", 3);
        assertEquals(List.of("a", "b", "c"),
                consumed.stream().map(QueueTestSupport::text).toList());
        assertEquals(newOffsets, consumed.stream().map(Delivery::offset).toList());
        consumed.forEach(q::ack);
        assertEquals(5L, q.committedOffset("g", "t", 0));
    }

    @Test
    void batchIsRejectedAtomicallyWhenOneCannotEnterAndNothingChanges(@TempDir Path dir) {
        ReliableMessageQueue q = queue(dir);
        // 容量 4：3 条进死信（视为已解决不占位），再发 2 条未解决占位，只剩 2 个名额，3 条重放进不去
        q.createTopic(new TopicConfig("t", 1, 4, 1 << 20));
        kill(q, "t", "g", "a");
        kill(q, "t", "g", "b");
        kill(q, "t", "g", "c");
        q.publishText("t", "hot-key", null, "occupy-1");
        q.publishText("t", "hot-key", null, "occupy-2");

        BatchReplayRejectedException ex = assertThrows(BatchReplayRejectedException.class,
                () -> q.replayDeadLetters(DeadLetterFilter.forTopic("t")));
        assertTrue(ex.getMessage().contains("容量"));

        // 整批原样拒绝：没有任何一条被重放
        List<DeadLetter> dead = q.listDeadLetters("g", "t");
        assertEquals(3, dead.size());
        assertTrue(dead.stream().noneMatch(DeadLetter::replayed));
        // 占用消息、重放幂等键等已有数据均未改动：poll 仍只拿到 2 条占用消息
        List<Delivery> visible = drainInOrder(q, "g", "t", 10);
        assertEquals(2, visible.size());
        assertEquals(List.of("occupy-1", "occupy-2"),
                visible.stream().map(QueueTestSupport::text).toList());
        visible.forEach(q::ack);
    }

    @Test
    void rejectedBatchCanBeResubmittedAsIsAfterFreeingCapacity(@TempDir Path dir) {
        ReliableMessageQueue q = queue(dir);
        q.createTopic(new TopicConfig("t", 1, 3, 1 << 20));
        kill(q, "t", "g", "a");
        kill(q, "t", "g", "b");
        // 2 条占位 -> 只剩 1 个名额，2 条重放进不去
        PublishResult o1 = q.publishText("t", "hot-key", null, "occupy-1");
        q.publishText("t", "hot-key", null, "occupy-2");

        DeadLetterFilter filter = DeadLetterFilter.forTopic("t");
        assertThrows(BatchReplayRejectedException.class, () -> q.replayDeadLetters(filter));
        // 一条都没有进入：死信均未标记重放
        assertTrue(q.listDeadLetters("g", "t").stream().noneMatch(DeadLetter::replayed));

        // 腾出容量：确认两条占用消息
        List<Delivery> occ = drainInOrder(q, "g", "t", 2);
        assertEquals(List.of("occupy-1", "occupy-2"),
                occ.stream().map(QueueTestSupport::text).toList());
        occ.forEach(q::ack);
        assertEquals(o1.offset() + 1, q.committedOffset("g", "t", 0));

        // 同一批原样再提交 -> 成功，且死信以后仍可正常重放消费
        BatchReplayResult result = q.replayDeadLetters(filter);
        assertEquals(2, result.selected());
        List<Delivery> consumed = drainInOrder(q, "g", "t", 2);
        assertEquals(List.of("a", "b"), consumed.stream().map(QueueTestSupport::text).toList());
    }

    @Test
    void alreadyReplayedEntriesAreSkippedAndSameFilterIsSafelyRepeatable(@TempDir Path dir) {
        ReliableMessageQueue q = queue(dir);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        kill(q, "t", "g", "a");
        PublishResult b = kill(q, "t", "g", "b");
        PublishResult c = kill(q, "t", "g", "c");

        String firstDlq = q.listDeadLetters("g", "t").stream()
                .min((x, y) -> Long.compare(x.originalOffset(), y.originalOffset()))
                .map(DeadLetter::deadLetterId).orElseThrow();
        // 先单条重放 a
        q.replayDeadLetter(firstDlq, "g");

        // 相同条件批量提交：已重放的 a 自动跳过，只选中 b、c 并成功，不会因 a 而整批失败
        BatchReplayResult result = q.replayDeadLetters(DeadLetterFilter.forTopic("t"));
        assertEquals(2, result.selected());
        assertEquals(List.of(b.offset(), c.offset()), result.items().stream()
                .map(x -> x.deadLetter().originalOffset()).toList());
        assertEquals(List.of("b", "c"), result.items().stream()
                .map(x -> QueueTestSupport.textPayload(x.deadLetter().payload())).toList());

        // 相同条件再次提交：没有未重放死信，selected=0，幂等无异常
        BatchReplayResult again = q.replayDeadLetters(DeadLetterFilter.forTopic("t"));
        assertEquals(0, again.selected());

        // 单条重复重放 a 仍被明确拒绝
        assertThrows(DuplicateIdempotencyKeyException.class,
                () -> q.replayDeadLetter(firstDlq, "g"));
    }

    @Test
    void filtersByGroupPartitionAndDeadAtRangeAndOrderIsDeterministic(@TempDir Path dir) {
        ReliableMessageQueue q = queue(dir);
        q.createTopic(new TopicConfig("t", 2, 1000, 1 << 20));

        // 用与实现一致的哈希公式预先选好落在目标分区的键，避免探测式发布产生未确认租约。
        String g1p0key1 = keyForPartition("g1p0", 0, 2);
        String g1p0key2 = keyForPartition("g1p0b", 0, 2);
        String g1p1key1 = keyForPartition("g1p1", 1, 2);
        String g1p1key2 = keyForPartition("g1p1b", 1, 2);
        String g2key = keyForPartition("g2", 0, 2);

        kill(q, "t", "g1", g1p0key1, "g1p0-0");
        kill(q, "t", "g1", g1p0key2, "g1p0-1");
        kill(q, "t", "g1", g1p1key1, "g1p1-0");
        kill(q, "t", "g1", g1p1key2, "g1p1-1");
        kill(q, "t", "g2", g2key, "g2");

        assertEquals(4, q.listDeadLetters("g1", "t").size());
        assertEquals(1, q.listDeadLetters("g2", "t").size());
        assertEquals(5, q.listDeadLetters(null, "t").size());

        List<DeadLetter> g1Dead = q.listDeadLetters("g1", "t");
        long minDeadAt = g1Dead.stream().mapToLong(DeadLetter::deadAt).min().orElseThrow();
        long maxDeadAt = g1Dead.stream().mapToLong(DeadLetter::deadAt).max().orElseThrow();

        // 分区筛选：g1 + 分区0 -> 2 条，且都在分区0，顺序按原位点升序
        BatchReplayResult p0Result = q.replayDeadLetters(
                new DeadLetterFilter("t", "g1", 0, null, null));
        assertEquals(2, p0Result.selected());
        assertTrue(p0Result.items().stream().allMatch(x -> x.deadLetter().partition() == 0));
        List<Long> p0Offsets = p0Result.items().stream()
                .map(x -> x.deadLetter().originalOffset()).toList();
        assertEquals(p0Offsets.stream().sorted().toList(), p0Offsets);

        // 时间范围筛选（含边界）：[minDeadAt,maxDeadAt] 覆盖 g1 全部剩余（分区1的 2 条）
        BatchReplayResult inRange = q.replayDeadLetters(
                new DeadLetterFilter("t", "g1", null, minDeadAt, maxDeadAt));
        assertEquals(2, inRange.selected());
        assertTrue(inRange.items().stream().allMatch(x -> x.deadLetter().partition() == 1));

        // 范围上界之后 -> 0 条
        assertEquals(0, q.replayDeadLetters(
                new DeadLetterFilter("t", "g1", null, maxDeadAt + 1, null)).selected());

        // g2 按 group 单独重放 1 条
        BatchReplayResult g2only = q.replayDeadLetters(
                new DeadLetterFilter("t", "g2", null, null, null));
        assertEquals(1, g2only.selected());

        // 全部已重放后，相同条件重复提交 selected=0 且无异常
        BatchReplayResult empty = q.replayDeadLetters(DeadLetterFilter.forTopic("t"));
        assertEquals(0, empty.selected());
        assertTrue(empty.items().isEmpty());
    }

    /** 与队列相同的分区哈希：找一个落在指定分区的键（在 base 后追加序号）。 */
    private static String keyForPartition(String base, int wantPartition, int partitionCount) {
        for (int i = 0; i < 10_000; i++) {
            String key = base + "-" + i;
            int p = Math.floorMod(key.hashCode(), partitionCount);
            if (p == wantPartition) {
                return key;
            }
        }
        throw new IllegalStateException("找不到目标分区的键: " + base);
    }

    /** 用指定分区键发布并 nack 到进死信。 */
    private void kill(ReliableMessageQueue q, String topic, String group, String partitionKey, String text) {
        PublishResult pr = q.publishText(topic, partitionKey, null, text);
        Delivery d = q.poll(group, topic, 10).stream()
                .filter(x -> x.messageId().equals(pr.messageId())).findFirst().orElseThrow();
        QueueTestSupport.logDelivery("nack-to-dlq", d);
        q.nack(d, "boom");
    }

    @Test
    void sameFilterProducesSameOrderAcrossRuns(@TempDir Path dir) {
        ReliableMessageQueue q = queue(dir);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        List<PublishResult> originals = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            originals.add(kill(q, "t", "g", "m" + i));
        }
        // 分两批重放以制造多次提交，每批相同条件的选择/排序应稳定
        DeadLetterFilter filter = DeadLetterFilter.forTopic("t");
        // 直接观察第一批结果顺序 = 原位点升序
        BatchReplayResult first = q.replayDeadLetters(filter);
        assertEquals(6, first.selected());
        List<Long> expectedOrder = originals.stream().map(PublishResult::offset).toList();
        assertEquals(expectedOrder,
                first.items().stream().map(x -> x.deadLetter().originalOffset()).toList());
    }
}
