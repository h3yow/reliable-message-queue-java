package com.github.h3yow.reliablemessagequeuejava.queue;

import com.github.h3yow.reliablemessagequeuejava.queue.exception.BatchReplayRejectedException;
import com.github.h3yow.reliablemessagequeuejava.queue.exception.DuplicateIdempotencyKeyException;
import com.github.h3yow.reliablemessagequeuejava.queue.exception.TopicCapacityExceededException;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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

    /** 反复 poll 并<em>确认</em>所有投递，直到该组在主题上没有可投递消息为止（清空在途租约）。 */
    private void drainAndAckAll(ReliableMessageQueue q, String group, String topic) {
        int guard = 0;
        while (guard++ < 50) {
            List<Delivery> batch = q.poll(group, topic, 50);
            if (batch.isEmpty()) {
                return;
            }
            batch.forEach(q::ack);
        }
        throw new AssertionError("清空消费组在途消息超时: " + group);
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
        // 容量 3：3 条进死信（已解决不占位），再发 2 条未解决占位，只剩 1 个名额，3 条重放进不去
        q.createTopic(new TopicConfig("t", 1, 3, 1 << 20));
        kill(q, "t", "g", "a");
        kill(q, "t", "g", "b");
        kill(q, "t", "g", "c");
        PublishResult o1 = q.publishText("t", "hot-key", null, "occupy-1");
        q.publishText("t", "hot-key", null, "occupy-2");

        // 拒绝前快照
        int messagesBefore = q.messageCount("t");
        long committedBefore = q.committedOffset("g", "t", 0);
        int replayedBefore = (int) q.listDeadLetters("g", "t").stream()
                .filter(DeadLetter::replayed).count();

        BatchReplayRejectedException ex = assertThrows(BatchReplayRejectedException.class,
                () -> q.replayDeadLetters(DeadLetterFilter.forTopic("t")));
        assertTrue(ex.getMessage().contains("容量"));

        // 整批原样拒绝——五项均不变：
        // 1) 消息数量不变（没有追加任何重放副本）
        assertEquals(messagesBefore, q.messageCount("t"));
        // 2) 死信状态不变：仍 3 条且全部未重放
        List<DeadLetter> dead = q.listDeadLetters("g", "t");
        assertEquals(3, dead.size());
        assertTrue(dead.stream().noneMatch(DeadLetter::replayed));
        assertEquals(replayedBefore, 0);
        // 3) 已确认进度不推进也不回退
        assertEquals(committedBefore, q.committedOffset("g", "t", 0));
        // 4) 容量占用不变：拒绝后仍恰好只有 2 个名额被占——再发布 1 条（占满）可以、
        //    紧接着第 2 条（probe-1 尚未确认、仍占位）必被容量拒绝
        PublishResult probe = q.publishText("t", "hot-key", null, "probe-1");
        assertEquals(o1.offset() + 2, probe.offset());
        assertThrows(TopicCapacityExceededException.class,
                () -> q.publishText("t", "hot-key", null, "probe-2"));
        // 5) 重试/在途状态不变：一次 poll 拿回 2 条占用消息 + probe，全部首次投递 attempt=1
        List<Delivery> visible = drainInOrder(q, "g", "t", 10);
        assertEquals(3, visible.size());
        assertEquals(List.of(o1.offset(), o1.offset() + 1, probe.offset()),
                visible.stream().map(Delivery::offset).toList());
        assertEquals(List.of("occupy-1", "occupy-2", "probe-1"),
                visible.stream().map(QueueTestSupport::text).toList());
        assertTrue(visible.stream().allMatch(d -> d.attempt() == 1));
        visible.forEach(q::ack);
    }

    @Test
    void rejectedBatchCanBeResubmittedAsIsAfterFreeingCapacity(@TempDir Path dir) {
        ReliableMessageQueue q = queue(dir);
        q.createTopic(new TopicConfig("t", 1, 3, 1 << 20));
        PublishResult a = kill(q, "t", "g", "a");
        PublishResult b = kill(q, "t", "g", "b");
        // 2 条占位 -> 只剩 1 个名额，2 条重放进不去
        PublishResult o1 = q.publishText("t", "hot-key", null, "occupy-1");
        q.publishText("t", "hot-key", null, "occupy-2");

        DeadLetterFilter filter = DeadLetterFilter.forTopic("t");
        // 拒绝前快照
        int messagesBefore = q.messageCount("t");
        long committedBefore = q.committedOffset("g", "t", 0);

        assertThrows(BatchReplayRejectedException.class, () -> q.replayDeadLetters(filter));
        // 一条都没有进入：死信均未标记重放
        assertTrue(q.listDeadLetters("g", "t").stream().noneMatch(DeadLetter::replayed));
        // 拒绝后消息数量/已确认进度不变
        assertEquals(messagesBefore, q.messageCount("t"));
        assertEquals(committedBefore, q.committedOffset("g", "t", 0));

        // 腾出容量：确认两条占用消息（拒绝后它们仍是首次投递 attempt=1，重试状态未变）
        List<Delivery> occ = drainInOrder(q, "g", "t", 2);
        assertEquals(List.of("occupy-1", "occupy-2"),
                occ.stream().map(QueueTestSupport::text).toList());
        assertTrue(occ.stream().allMatch(d -> d.attempt() == 1));
        occ.forEach(q::ack);
        assertEquals(o1.offset() + 1, q.committedOffset("g", "t", 0));

        // 完全相同的筛选条件原样再提交 -> 整体成功（不是部分成功）
        BatchReplayResult result = q.replayDeadLetters(filter);
        assertEquals(2, result.selected());
        // 同分区按进死信前先后顺序：a 然后 b；新位点连续追加
        assertEquals(List.of(a.offset(), b.offset()), result.items().stream()
                .map(x -> x.deadLetter().originalOffset()).toList());
        // 消费顺序与选择顺序一致、可复现
        List<Delivery> consumed = drainInOrder(q, "g", "t", 2);
        assertEquals(List.of("a", "b"), consumed.stream().map(QueueTestSupport::text).toList());
        assertEquals(result.items().stream().map(x -> x.publishResult().offset()).toList(),
                consumed.stream().map(Delivery::offset).toList());
        consumed.forEach(q::ack);
        // 旧的已确认进度没有被回退：最终位点推进到重放副本的最后位点
        assertEquals(consumed.get(1).offset(), q.committedOffset("g", "t", 0));

        // 成功后再用同一条件提交：命中已重放记录，整批拒绝而不是部分执行
        assertThrows(BatchReplayRejectedException.class, () -> q.replayDeadLetters(filter));
    }

    @Test
    void batchCoveringAnAlreadyReplayedDeadLetterIsRejectedWholeAndNothingChanges(@TempDir Path dir) {
        ReliableMessageQueue q = queue(dir);
        // 2 分区：a 落在分区0，b、c 落在分区1，便于用分区筛选确定性地“缩小条件”
        q.createTopic(new TopicConfig("t", 2, 100, 1 << 20));
        String keyP0 = keyForPartition("a-key", 0, 2);
        String keyP1 = keyForPartition("bc-key", 1, 2);
        kill(q, "t", "g", keyP0, "a");
        kill(q, "t", "g", keyP1, "b");
        kill(q, "t", "g", keyP1, "c");

        List<DeadLetter> deadBefore = q.listDeadLetters(null, "t").stream()
                .sorted(Comparator.comparingLong(DeadLetter::originalOffset)).toList();
        String dlqA = deadBefore.get(0).deadLetterId();
        long offsetB = deadBefore.get(1).originalOffset();
        long offsetC = deadBefore.get(2).originalOffset();

        // 先单独重放 a：a 的副本进入分区0投递流程，死信 a 标记已重放
        q.replayDeadLetter(dlqA, "g");
        List<Delivery> replayA = drainInOrder(q, "g", "t", 1);
        assertEquals(List.of("a"), replayA.stream().map(QueueTestSupport::text).toList());
        replayA.forEach(q::ack);
        long committedP0AfterSingle = q.committedOffset("g", "t", 0);
        int messageCountAfterSingle = countMessages(q, "t");

        // 再用覆盖同一主题全部死信（含已重放 a）的条件批量重放：整批必须原样拒绝，
        // 不能跳过 a 只重放 b、c。
        BatchReplayRejectedException ex = assertThrows(BatchReplayRejectedException.class,
                () -> q.replayDeadLetters(DeadLetterFilter.forTopic("t")));
        assertTrue(ex.getMessage().contains("已重放"));
        assertTrue(ex.getMessage().contains(dlqA));

        // 拒绝后状态完全不变：
        // 1) 死信状态：a 仍为已重放，b、c 仍未重放
        java.util.Map<String, DeadLetter> byId = q.listDeadLetters(null, "t").stream()
                .collect(java.util.stream.Collectors.toMap(DeadLetter::deadLetterId, x -> x));
        assertTrue(byId.get(dlqA).replayed());
        assertEquals(2, byId.values().stream().filter(d -> !d.replayed()).count());
        // 2) 消息数量不变（没有为 b、c 追加任何重放副本）
        assertEquals(messageCountAfterSingle, countMessages(q, "t"));
        // 3) 已确认进度不推进也不回退（分区0 的进度停在单条重放确认之后）
        assertEquals(committedP0AfterSingle, q.committedOffset("g", "t", 0));
        // 4) 重试/投递状态不变：b、c 的重放副本没有进入投递流程，poll 不到任何消息
        assertTrue(drainInOrder(q, "g", "t", 10).isEmpty());

        // 缩小筛选条件（只选分区1 上的 b、c）后可以整体成功，顺序按原位点升序
        BatchReplayResult narrowed = q.replayDeadLetters(
                new DeadLetterFilter("t", null, 1, null, null));
        assertEquals(2, narrowed.selected());
        assertEquals(List.of(offsetB, offsetC), narrowed.items().stream()
                .map(x -> x.deadLetter().originalOffset()).toList());
        assertEquals(List.of("b", "c"), narrowed.items().stream()
                .map(x -> QueueTestSupport.textPayload(x.deadLetter().payload())).toList());
        List<Delivery> consumed = drainInOrder(q, "g", "t", 2);
        assertEquals(List.of("b", "c"), consumed.stream().map(QueueTestSupport::text).toList());

        // 单条重复重放 a 仍被明确拒绝
        assertThrows(DuplicateIdempotencyKeyException.class,
                () -> q.replayDeadLetter(dlqA, "g"));
    }

    /** 统计主题内消息总数（用于验证整批被拒后消息数量不变）。 */
    private static int countMessages(ReliableMessageQueue q, String topic) {
        return q.messageCount(topic);
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
        long maxDeadAt = g1Dead.stream().mapToLong(DeadLetter::deadAt).max().orElseThrow();

        // 分区筛选：g1 + 分区0 -> 2 条，且都在分区0，顺序按原位点升序
        BatchReplayResult p0Result = q.replayDeadLetters(
                new DeadLetterFilter("t", "g1", 0, null, null));
        assertEquals(2, p0Result.selected());
        assertTrue(p0Result.items().stream().allMatch(x -> x.deadLetter().partition() == 0));
        List<Long> p0Offsets = p0Result.items().stream()
                .map(x -> x.deadLetter().originalOffset()).toList();
        assertEquals(p0Offsets.stream().sorted().toList(), p0Offsets);

        // 时间范围筛选（含边界）：g1 + 分区1 + [分区1死信的最早/最晚 deadAt] -> 分区1 的 2 条。
        // 注意必须同时限定分区1：宽时间范围会命中分区0已重放记录，新规则下那样会整批拒绝。
        List<DeadLetter> g1P1Dead = g1Dead.stream().filter(d -> d.partition() == 1).toList();
        long p1Min = g1P1Dead.stream().mapToLong(DeadLetter::deadAt).min().orElseThrow();
        long p1Max = g1P1Dead.stream().mapToLong(DeadLetter::deadAt).max().orElseThrow();
        BatchReplayResult inRange = q.replayDeadLetters(
                new DeadLetterFilter("t", "g1", 1, p1Min, p1Max));
        assertEquals(2, inRange.selected());
        assertTrue(inRange.items().stream().allMatch(x -> x.deadLetter().partition() == 1));

        // 范围上界之后 -> 0 条
        assertEquals(0, q.replayDeadLetters(
                new DeadLetterFilter("t", "g1", null, maxDeadAt + 1, null)).selected());

        // g2 按 group 单独重放 1 条
        BatchReplayResult g2only = q.replayDeadLetters(
                new DeadLetterFilter("t", "g2", null, null, null));
        assertEquals(1, g2only.selected());

        // 全部已重放后，再用覆盖这些死信的宽条件提交：命中已重放记录，整批拒绝（不会部分执行）
        BatchReplayRejectedException ex = assertThrows(BatchReplayRejectedException.class,
                () -> q.replayDeadLetters(DeadLetterFilter.forTopic("t")));
        assertTrue(ex.getMessage().contains("已重放"));
        // 同样地，按组重复提交（范围内是该组已重放的死信）也整批拒绝
        assertThrows(BatchReplayRejectedException.class,
                () -> q.replayDeadLetters(new DeadLetterFilter("t", "g2", null, null, null)));

        // 而筛选范围内一条死信都没有（全主题最后一条死信之后的未来下界）仍是无操作：
        // selected=0 且不抛异常（注意区别于“命中已重放记录”的整批拒绝）
        long allMaxDeadAt = q.listDeadLetters(null, "t").stream()
                .mapToLong(DeadLetter::deadAt).max().orElseThrow();
        BatchReplayResult none = q.replayDeadLetters(
                new DeadLetterFilter("t", null, null, allMaxDeadAt + 1, null));
        assertEquals(0, none.selected());
        assertTrue(none.items().isEmpty());
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

    /** 用指定分区键发布并 nack 到进死信；队首阻塞时反复 poll 直到取回该消息（不确认其他消息）。 */
    private void kill(ReliableMessageQueue q, String topic, String group, String partitionKey, String text) {
        PublishResult pr = q.publishText(topic, partitionKey, null, text);
        Delivery d = awaitDelivery(q, group, topic, pr.messageId());
        QueueTestSupport.logDelivery("nack-to-dlq", d);
        q.nack(d, "boom");
    }

    /** 反复 poll 直到取回指定 messageId 的投递（其他租约到期后会回收，不影响目标消息）。 */
    private static Delivery awaitDelivery(ReliableMessageQueue q, String group, String topic,
                                          String messageId) {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            for (Delivery d : q.poll(group, topic, 10)) {
                if (d.messageId().equals(messageId)) {
                    return d;
                }
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
        throw new java.util.NoSuchElementException("等不到目标消息的投递: " + messageId);
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

    @Test
    void identicalLayoutAndFilterProducesIdenticalReplayOrderAcrossIndependentQueues(@TempDir Path dir) {
        // 两个独立目录，构造相同的死信布局（同键、同顺序、多分区多消费组），
        // 用完全相同的筛选条件批量重放：追加位点顺序与消费顺序必须一致可复现。
        List<List<Delivery>> orders = new ArrayList<>();
        for (int round = 0; round < 2; round++) {
            java.nio.file.Path roundDir = dir.resolve("round-" + round);
            ReliableMessageQueue q = queue(roundDir);
            q.createTopic(new TopicConfig("t", 2, 1000, 1 << 20));
            // g1：分区0 两条、分区1 两条
            for (String key : List.of(keyForPartition("g1p0a", 0, 2),
                    keyForPartition("g1p0b", 0, 2),
                    keyForPartition("g1p1a", 1, 2),
                    keyForPartition("g1p1b", 1, 2))) {
                kill(q, "t", "g1", key, "g1:" + key);
            }
            // g2 是独立消费组：它也会消费全主题的 4 条原消息，先全部解决，再制造自己的 2 条死信
            drainAndAckAll(q, "g2", "t");
            for (String key : List.of(keyForPartition("g2p0a", 0, 2),
                    keyForPartition("g2p0b", 0, 2))) {
                kill(q, "t", "g2", key, "g2:" + key);
            }
            // g2 弄死自己 2 条后，g1 也需要消费掉这 2 条，避免它们在 g1 侧挡住 g1 的重放副本
            drainAndAckAll(q, "g1", "t");
            // 相同筛选条件：只重放 g1 全部
            BatchReplayResult result = q.replayDeadLetters(
                    new DeadLetterFilter("t", "g1", null, null, null));
            assertEquals(4, result.selected());
            // 排序键：分区升序 -> 原位点升序
            List<String> orderKeys = result.items().stream()
                    .map(x -> x.deadLetter().partition() + ":" + x.deadLetter().originalOffset())
                    .toList();
            assertEquals(orderKeys.stream().sorted().toList(), orderKeys,
                    "结果必须按 分区->原位点 排序");
            // 实际消费顺序：只保留本次重放副本（按 messageId 过滤）
            java.util.Set<String> replayIds = result.items().stream()
                    .map(x -> x.publishResult().messageId()).collect(java.util.stream.Collectors.toSet());
            List<Delivery> consumed = drainInOrder(q, "g1", "t", 50).stream()
                    .filter(d -> replayIds.contains(d.messageId())).toList();
            orders.add(consumed);
            assertEquals(4, consumed.size());
            // 按分区→位点排序后，消费到的重放副本与批量结果一一对应
            List<Long> replayOffsetsSorted = result.items().stream()
                    .map(x -> x.publishResult().offset()).toList();
            List<Long> consumedOffsetsSorted = consumed.stream()
                    .sorted(Comparator.comparingInt(Delivery::partition)
                            .thenComparingLong(Delivery::offset))
                    .map(Delivery::offset).toList();
            assertEquals(replayOffsetsSorted, consumedOffsetsSorted);
        }
        // 两轮独立执行，每个分区内消费到的原位点/负载序列完全一致（跨分区由 poll 轮询顺序决定）
        java.util.function.Function<List<Delivery>, List<String>> perPartition = list -> list.stream()
                .collect(java.util.stream.Collectors.groupingBy(Delivery::partition,
                        java.util.TreeMap::new,
                        java.util.stream.Collectors.mapping(
                                x -> QueueTestSupport.text(x) + "@" + x.offset(),
                                java.util.stream.Collectors.toList())))
                .values().stream().flatMap(List::stream).toList();
        List<String> r1 = perPartition.apply(orders.get(0));
        List<String> r2 = perPartition.apply(orders.get(1));
        assertEquals(r1, r2, "多次执行相同条件时各分区内顺序要一致可复现");
    }

    @Test
    void batchReplayCoversMultipleGroupsAndEachGroupConsumesCopiesInOrder(@TempDir Path dir) {
        ReliableMessageQueue q = queue(dir);
        q.createTopic(new TopicConfig("t", 2, 1000, 1 << 20));
        // 两组各自把不同分区上的消息弄进死信（消息本身属于主题，不属于某个组）
        String g1key = keyForPartition("g1dead", 0, 2);
        String g2key = keyForPartition("g2dead", 1, 2);
        kill(q, "t", "g1", g1key, "g1-dead");
        // 让 g2 先消费掉 g1 弄死的那条，避免后面制造 g2 死信时被它挡住
        Delivery g2ConsumesG1Dead = awaitDelivery(q, "g2", "t",
                q.listDeadLetters("g1", "t").get(0).originalMessageId());
        q.ack(g2ConsumesG1Dead);
        // 再由 g2 弄死分区1 上的消息
        kill(q, "t", "g2", g2key, "g2-dead");
        // g1 也消费掉 g2 弄死的那条原消息（对 g1 而言它只是普通消息）
        Delivery g1ConsumesG2Dead = awaitDelivery(q, "g1", "t",
                q.listDeadLetters("g2", "t").get(0).originalMessageId());
        q.ack(g1ConsumesG2Dead);

        // 按主题（不指定组）批量重放：两组的死信都入选
        BatchReplayResult result = q.replayDeadLetters(DeadLetterFilter.forTopic("t"));
        assertEquals(2, result.selected());
        List<String> replayGroups = result.items().stream()
                .map(x -> x.deadLetter().group()).sorted().toList();
        assertEquals(List.of("g1", "g2"), replayGroups);

        // 两组各自消费全主题，都会拿到两条重放副本。同分区内按原位点顺序（各只有 1 条/分区），
        // 跨分区由 poll 轮询决定，因此按分区分组断言内容与位点。
        for (String group : List.of("g1", "g2")) {
            List<Delivery> consumed = drainInOrder(q, group, "t", 2);
            assertEquals(2, consumed.size());
            Map<Integer, List<Delivery>> byPartition = consumed.stream()
                    .collect(java.util.stream.Collectors.groupingBy(Delivery::partition,
                            java.util.TreeMap::new, java.util.stream.Collectors.toList()));
            assertEquals(List.of("g1-dead"), byPartition.get(0).stream()
                    .map(QueueTestSupport::text).toList(), group + " 分区0");
            assertEquals(List.of("g2-dead"), byPartition.get(1).stream()
                    .map(QueueTestSupport::text).toList(), group + " 分区1");
            consumed.forEach(q::ack);
            // 已确认进度只进不退：分区1 最终位点推进到第二条副本
            assertEquals(byPartition.get(1).get(0).offset(), q.committedOffset(group, "t", 1));
        }
    }
}
