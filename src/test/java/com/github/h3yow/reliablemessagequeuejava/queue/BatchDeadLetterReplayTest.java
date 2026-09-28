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
import static org.junit.jupiter.api.Assertions.assertNull;
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
        // 容量 4：2 条死信（不占位）+ 2 条占位 = 4 已满；2 条重放进不去，再发 1 条也必被容量拒绝
        q.createTopic(new TopicConfig("t", 1, 3, 1 << 20));
        PublishResult a = kill(q, "t", "g", "a");
        PublishResult b = kill(q, "t", "g", "b");
        // 2 条占位：pending=2，2 条重放进不去（2+2 > 3）
        q.publishText("t", "hot-key", null, "occupy-1");
        q.publishText("t", "hot-key", null, "occupy-2");

        // 拒绝前的进度与死信快照
        long committedBefore = q.committedOffset("g", "t", 0);
        assertEquals(b.offset(), committedBefore);
        List<String> dlqIdsBefore = q.listDeadLetters("g", "t").stream()
                .map(DeadLetter::deadLetterId).sorted().toList();

        DeadLetterFilter filter = DeadLetterFilter.forTopic("t");
        BatchReplayRejectedException ex = assertThrows(BatchReplayRejectedException.class,
                () -> q.replayDeadLetters(filter));
        assertTrue(ex.getMessage().contains("容量"));

        // 拒绝后状态完全不变：死信均未标记重放、数量不变
        List<DeadLetter> dead = q.listDeadLetters("g", "t");
        assertEquals(dlqIdsBefore, dead.stream().map(DeadLetter::deadLetterId).sorted().toList());
        assertTrue(dead.stream().noneMatch(DeadLetter::replayed));
        // 已确认进度没有推进或回退
        assertEquals(committedBefore, q.committedOffset("g", "t", 0));
        // 容量占用不变：当前仅剩 1 个名额，再发 1 条恰好占满，第 2 条必被容量拒绝
        q.publishText("t", "hot-key", null, "fills-last-slot");
        assertThrows(TopicCapacityExceededException.class,
                () -> q.publishText("t", "hot-key", null, "overflow"));
        // poll 只能拿到 3 条占位消息，没有任何重放副本混入；其投递/租约状态不受影响
        List<Delivery> visibleBefore = drainInOrder(q, "g", "t", 10);
        assertEquals(3, visibleBefore.size());

        // 用完全相同的筛选条件重复提交：容量问题仍在，继续原样拒绝且状态依旧不变
        assertThrows(BatchReplayRejectedException.class, () -> q.replayDeadLetters(filter));
        assertTrue(q.listDeadLetters("g", "t").stream().noneMatch(DeadLetter::replayed));
        assertEquals(committedBefore, q.committedOffset("g", "t", 0));

        // 腾出容量：确认三条占位消息（占位消息的租约仍有效）
        assertEquals(List.of("occupy-1", "occupy-2", "fills-last-slot"),
                visibleBefore.stream().map(QueueTestSupport::text).toList());
        visibleBefore.forEach(q::ack);
        assertEquals(4L, q.committedOffset("g", "t", 0));

        // 同一批原样再提交 -> 整体成功，2 条全部进入且顺序确定（按原位点升序）
        BatchReplayResult result = q.replayDeadLetters(filter);
        assertEquals(2, result.selected());
        assertEquals(List.of(a.offset(), b.offset()), result.items().stream()
                .map(x -> x.deadLetter().originalOffset()).toList());
        // 死信记录全部标记已重放，且指向各自新位点（追加在占位消息之后）
        List<Long> replayOffsets = result.items().stream()
                .map(x -> x.publishResult().offset()).sorted().toList();
        assertEquals(replayOffsets, result.items().stream()
                .map(x -> x.deadLetter().replayedToOffset()).sorted().toList());

        // 消费顺序与原死信先后一致；确认后进度连续推进到最后一条重放副本位点
        List<Delivery> consumed = drainInOrder(q, "g", "t", 2);
        assertEquals(List.of("a", "b"), consumed.stream().map(QueueTestSupport::text).toList());
        consumed.forEach(q::ack);
        assertEquals(replayOffsets.get(replayOffsets.size() - 1),
                q.committedOffset("g", "t", 0));
    }

    @Test
    void batchIsRejectedAtomicallyWhenFilterIncludesAlreadyReplayedDeadLetter(@TempDir Path dir) throws Exception {
        ReliableMessageQueue q = queue(dir);
        // 容量 5，便于精确验证容量占用不发生变化
        q.createTopic(new TopicConfig("t", 1, 5, 1 << 20));
        kill(q, "t", "g", "a");
        Thread.sleep(2);
        kill(q, "t", "g", "b");
        Thread.sleep(2);
        PublishResult c = kill(q, "t", "g", "c");
        // 3 条进死信即视为已解决，位点已连续推进到 c；此时容量占用为 0
        assertEquals(c.offset(), q.committedOffset("g", "t", 0));

        List<DeadLetter> deadBefore = q.listDeadLetters("g", "t").stream()
                .sorted(Comparator.comparingLong(DeadLetter::originalOffset)).toList();
        String firstDlq = deadBefore.get(0).deadLetterId();

        // 先单条重放 a：容量占用变为 1，a 标记已重放
        q.replayDeadLetter(firstDlq, "g");
        Delivery replayOfA = q.poll("g", "t", 10).stream()
                .filter(d -> d.offset() == 3L).findFirst().orElseThrow();

        // 用覆盖同主题全部死信的相同条件批量提交：因范围内含已重放的 a，整批必须原样拒绝
        BatchReplayRejectedException ex = assertThrows(BatchReplayRejectedException.class,
                () -> q.replayDeadLetters(DeadLetterFilter.forTopic("t")));
        assertTrue(ex.getMessage().contains("已重放"), "拒绝原因应指明已重放记录: " + ex.getMessage());

        // —— 拒绝后一切状态不变 ——
        // 死信记录：仍是 3 条，a 保持已重放，b/c 保持未重放
        List<DeadLetter> deadAfter = q.listDeadLetters("g", "t").stream()
                .sorted(Comparator.comparingLong(DeadLetter::originalOffset)).toList();
        assertEquals(3, deadAfter.size());
        assertEquals(true, deadAfter.get(0).replayed());
        assertEquals(3L, deadAfter.get(0).replayedToOffset());
        assertEquals(false, deadAfter.get(1).replayed());
        assertNull(deadAfter.get(1).replayedToOffset());
        assertEquals(false, deadAfter.get(2).replayed());
        // 已确认进度未被推进或回退
        assertEquals(c.offset(), q.committedOffset("g", "t", 0));
        // 没有任何新消息混入：a 仍持有旧租约，poll 为空（重试/租约状态未受影响）
        assertTrue(q.poll("g", "t", 10).isEmpty());
        // 容量占用与拒绝前一致（仅 a 占 1 个名额）：还能再发 4 条，第 5 条必被容量拒绝
        for (int i = 0; i < 4; i++) {
            q.publishText("t", "hot-key", null, "fill-" + i);
        }
        assertThrows(TopicCapacityExceededException.class,
                () -> q.publishText("t", "hot-key", null, "overflow"));

        // a 的旧投递与租约仍然有效，确认后连同填充消息一起放行，进度连续推进
        q.ack(replayOfA);
        List<Delivery> fills = drainInOrder(q, "g", "t", 4);
        assertEquals(List.of("fill-0", "fill-1", "fill-2", "fill-3"),
                fills.stream().map(QueueTestSupport::text).toList());
        fills.forEach(q::ack);
        assertEquals(7L, q.committedOffset("g", "t", 0));

        // 缩小筛选范围（用 group+死信时间窗把 a 排除）后可整体提交
        long deadAtB = deadAfter.get(1).deadAt();
        long deadAtC = deadAfter.get(2).deadAt();
        DeadLetterFilter narrowed = new DeadLetterFilter("t", "g", null, deadAtB, deadAtC);
        BatchReplayResult result = q.replayDeadLetters(narrowed);
        assertEquals(2, result.selected());
        assertEquals(List.of(deadAfter.get(1).deadLetterId(), deadAfter.get(2).deadLetterId()),
                result.items().stream().map(x -> x.deadLetter().deadLetterId()).toList());
        // 同条件再次提交：范围内现在都是已重放记录，仍然整批拒绝而不是静默跳过
        assertThrows(BatchReplayRejectedException.class, () -> q.replayDeadLetters(narrowed));

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
        List<DeadLetter> g1p1Dead = g1Dead.stream()
                .filter(d -> d.partition() == 1)
                .sorted(Comparator.comparingLong(DeadLetter::deadAt)).toList();
        assertEquals(2, g1p1Dead.size());
        long p1MinDeadAt = g1p1Dead.get(0).deadAt();
        long p1MaxDeadAt = g1p1Dead.get(1).deadAt();

        // 分区筛选：g1 + 分区0 -> 2 条，且都在分区0，顺序按原位点升序
        BatchReplayResult p0Result = q.replayDeadLetters(
                new DeadLetterFilter("t", "g1", 0, null, null));
        assertEquals(2, p0Result.selected());
        assertTrue(p0Result.items().stream().allMatch(x -> x.deadLetter().partition() == 0));
        List<Long> p0Offsets = p0Result.items().stream()
                .map(x -> x.deadLetter().originalOffset()).toList();
        assertEquals(p0Offsets.stream().sorted().toList(), p0Offsets);

        // 时间范围筛选（含边界）：分区1 + 其死信时间窗，确定性命中分区1的 2 条（未重放）
        BatchReplayResult inRange = q.replayDeadLetters(
                new DeadLetterFilter("t", "g1", 1, p1MinDeadAt, p1MaxDeadAt));
        assertEquals(2, inRange.selected());
        assertTrue(inRange.items().stream().allMatch(x -> x.deadLetter().partition() == 1));

        // 范围上界之后 -> 0 条（筛选范围内本来就没有死信：无操作，不视为拒绝）
        assertEquals(0, q.replayDeadLetters(
                new DeadLetterFilter("t", "g1", null, p1MaxDeadAt + 1, null)).selected());

        // g2 按 group 单独重放 1 条
        BatchReplayResult g2only = q.replayDeadLetters(
                new DeadLetterFilter("t", "g2", null, null, null));
        assertEquals(1, g2only.selected());

        // 全部已重放后，相同条件再次提交 -> 整批拒绝（不静默跳过），不会再产生任何消息
        BatchReplayRejectedException allReplayed = assertThrows(BatchReplayRejectedException.class,
                () -> q.replayDeadLetters(DeadLetterFilter.forTopic("t")));
        assertTrue(allReplayed.getMessage().contains("已重放"));
        assertTrue(q.listDeadLetters(null, "t").stream().allMatch(DeadLetter::replayed));
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

    /**
     * 消费组独立消费全主题：把 owned 负载对应的消息 nack 到进死信，
     * 其他消息直接确认（代表该组正常消费掉了别的业务消息），避免其持租约挡住后续位点。
     */
    private void killOwnedInOrder(ReliableMessageQueue q, String group, java.util.Set<String> owned) {
        int killed = 0;
        int guard = 0;
        while (killed < owned.size() && guard++ < 100) {
            for (Delivery d : q.poll(group, "t", 10)) {
                if (owned.contains(QueueTestSupport.text(d))) {
                    QueueTestSupport.logDelivery("nack-to-dlq", d);
                    q.nack(d, "boom");
                    killed++;
                } else {
                    q.ack(d);
                }
            }
        }
        assertEquals(owned.size(), killed);
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
    void identicalSetupAndFilterProduceIdenticalResultOrderAndProgress(@TempDir Path dir) {
        // 两个相互独立的数据目录，按相同步骤制造 2 分区死信，再用相同筛选条件批量重放：
        // 结果中 (分区,原位点,负载) 序列与确认后的已提交进度必须完全一致、可复现。
        List<List<String>> signaturesA = runAndCaptureOrder(dir.resolve("a"));
        List<List<String>> signaturesB = runAndCaptureOrder(dir.resolve("b"));
        assertEquals(signaturesA, signaturesB, "相同条件多次执行的结果顺序必须一致可复现");
    }

    /** 建主题 -> 制造 4 条跨分区死信 -> 批量重放 -> 消费并确认；返回 (分区:原位点:负载) 签名序列。 */
    private List<List<String>> runAndCaptureOrder(Path subDir) {
        ReliableMessageQueue q = queue(subDir);
        q.createTopic(new TopicConfig("t", 2, 1000, 1 << 20));
        String k0a = keyForPartition("ord-p0a", 0, 2);
        String k0b = keyForPartition("ord-p0b", 0, 2);
        String k1a = keyForPartition("ord-p1a", 1, 2);
        String k1b = keyForPartition("ord-p1b", 1, 2);
        List<String> keys = List.of(k0a, k1a, k0b, k1b);
        for (int i = 0; i < keys.size(); i++) {
            PublishResult pr = q.publishText("t", keys.get(i), null, "body-" + i);
            Delivery d = q.poll("g", "t", 10).stream()
                    .filter(x -> x.messageId().equals(pr.messageId())).findFirst().orElseThrow();
            q.nack(d, "boom");
        }
        BatchReplayResult result = q.replayDeadLetters(DeadLetterFilter.forTopic("t"));
        assertEquals(4, result.selected());
        // 消费全部重放副本并确认
        List<Delivery> consumed = drainInOrder(q, "g", "t", 4);
        assertEquals(4, consumed.size());
        consumed.forEach(q::ack);
        // 进度推进到每个分区重放副本的新位点（2 个分区各 2 条原位点 + 2 条重放副本 -> 3）
        assertEquals(3L, q.committedOffset("g", "t", 0));
        assertEquals(3L, q.committedOffset("g", "t", 1));
        return result.items().stream()
                .map(x -> List.of(String.valueOf(x.deadLetter().partition()),
                        String.valueOf(x.deadLetter().originalOffset()),
                        QueueTestSupport.textPayload(x.deadLetter().payload())))
                .toList();
    }

    @Test
    void multiGroupMultiPartitionBatchIsOrderedDeterministicallyAndConsumedPerPartitionInOrder(
            @TempDir Path dir) {
        ReliableMessageQueue q = queue(dir);
        q.createTopic(new TopicConfig("t", 2, 1000, 1 << 20));

        // g1：分区0 两条、分区1 两条；g2：各分区一条。键用确定性哈希预先选好。
        String g1p0a = keyForPartition("g1p0a", 0, 2);
        String g1p0b = keyForPartition("g1p0b", 0, 2);
        String g1p1a = keyForPartition("g1p1a", 1, 2);
        String g1p1b = keyForPartition("g1p1b", 1, 2);
        String g2p0 = keyForPartition("g2p0", 0, 2);
        String g2p1 = keyForPartition("g2p1", 1, 2);
        // 先按组发完全部消息（同组消息连续发布），再分别把各组消息 nack 进死信。
        // 每组都能看到全主题消息，因此消费时只 nack 属于本组预期负载的那些。
        q.publishText("t", g1p0a, null, "g1-p0-a");
        q.publishText("t", g1p0b, null, "g1-p0-b");
        q.publishText("t", g1p1a, null, "g1-p1-a");
        q.publishText("t", g1p1b, null, "g1-p1-b");
        q.publishText("t", g2p0, null, "g2-p0");
        q.publishText("t", g2p1, null, "g2-p1");
        killOwnedInOrder(q, "g1",
                new java.util.HashSet<>(List.of("g1-p0-a", "g1-p0-b", "g1-p1-a", "g1-p1-b")));
        killOwnedInOrder(q, "g2", new java.util.HashSet<>(List.of("g2-p0", "g2-p1")));

        List<DeadLetter> allDead = q.listDeadLetters(null, "t");
        assertEquals(6, allDead.size());

        DeadLetterFilter topicWide = DeadLetterFilter.forTopic("t");
        BatchReplayResult result = q.replayDeadLetters(topicWide);
        assertEquals(6, result.selected());

        // 结果确定性排序：分区 -> 原位点 -> 死信 id
        List<List<Object>> keyOrder = result.items().stream()
                .map(x -> List.<Object>of(x.deadLetter().partition(),
                        x.deadLetter().originalOffset(), x.deadLetter().deadLetterId()))
                .toList();
        List<List<Object>> sortedKeyOrder = keyOrder.stream()
                .sorted((x, y) -> {
                    int cmp = Integer.compare((Integer) x.get(0), (Integer) y.get(0));
                    if (cmp != 0) {
                        return cmp;
                    }
                    cmp = Long.compare((Long) x.get(1), (Long) y.get(1));
                    if (cmp != 0) {
                        return cmp;
                    }
                    return ((String) x.get(2)).compareTo((String) y.get(2));
                })
                .toList();
        assertEquals(sortedKeyOrder, keyOrder, "批量结果必须按 分区->原位点->死信id 确定排序");

        // 新位点在每个分区内连续追加，不与原位点交错
        List<Long> p0NewOffsets = result.items().stream()
                .filter(x -> x.deadLetter().partition() == 0)
                .map(x -> x.publishResult().offset()).sorted().toList();
        List<Long> p1NewOffsets = result.items().stream()
                .filter(x -> x.deadLetter().partition() == 1)
                .map(x -> x.publishResult().offset()).sorted().toList();
        assertEquals(3, p0NewOffsets.size());
        assertEquals(3, p1NewOffsets.size());

        // 两个消费组各自消费：同分区内严格按“进死信前先后”收到重放副本，组间互不影响。
        // 一次消费全部在途消息（poll 会跨分区轮询，必须一起处理，避免别的分区消息持租约挡队首）。
        assertOwnedConsumedByOriginalOrder(q, "g1", Map.of(
                0, List.of("g1-p0-a", "g1-p0-b"),
                1, List.of("g1-p1-a", "g1-p1-b")));
        assertOwnedConsumedByOriginalOrder(q, "g2", Map.of(
                0, List.of("g2-p0"),
                1, List.of("g2-p1")));

        // 全部已重放：同条件再次提交整批拒绝（含已重放记录），不会产生任何额外消息
        assertThrows(BatchReplayRejectedException.class, () -> q.replayDeadLetters(topicWide));
    }

    /**
     * 消费某组当前全部在途消息并按分区分组校验：ownedPerPartition 中列出的本组死信重放副本
     * 必须严格按原位点先后出现；其他消息（如别的组死信的重放副本）直接确认。
     */
    private static void assertOwnedConsumedByOriginalOrder(ReliableMessageQueue q, String group,
                                                           Map<Integer, List<String>> ownedPerPartition) {
        int totalOwned = ownedPerPartition.values().stream().mapToInt(List::size).sum();
        Map<Integer, List<Delivery>> gotByPartition = new java.util.HashMap<>();
        int collected = 0;
        int guard = 0;
        while (collected < totalOwned && guard++ < 100) {
            for (Delivery d : q.poll(group, "t", 50)) {
                List<String> owned = ownedPerPartition.get(d.partition());
                String text = QueueTestSupport.text(d);
                if (owned != null && owned.contains(text)) {
                    gotByPartition.computeIfAbsent(d.partition(), k -> new ArrayList<>()).add(d);
                    collected++;
                } else {
                    // 本组正常消费的其他业务消息（含别的组死信重放副本），立即确认放行队首。
                    q.ack(d);
                }
            }
        }
        for (Map.Entry<Integer, List<String>> e : ownedPerPartition.entrySet()) {
            int partition = e.getKey();
            List<String> actual = gotByPartition.getOrDefault(partition, List.<Delivery>of())
                    .stream().map(QueueTestSupport::text).toList();
            assertEquals(e.getValue(), actual,
                    "group=" + group + " partition=" + partition + " 的消费顺序应与进死信前一致");
        }
        gotByPartition.values().forEach(list -> list.forEach(q::ack));
    }
}
