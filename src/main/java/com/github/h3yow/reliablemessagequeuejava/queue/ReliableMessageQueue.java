package com.github.h3yow.reliablemessagequeuejava.queue;

import com.github.h3yow.reliablemessagequeuejava.queue.exception.DuplicateAckException;
import com.github.h3yow.reliablemessagequeuejava.queue.exception.DuplicateIdempotencyKeyException;
import com.github.h3yow.reliablemessagequeuejava.queue.exception.InvalidTopicException;
import com.github.h3yow.reliablemessagequeuejava.queue.exception.OffsetRollbackException;
import com.github.h3yow.reliablemessagequeuejava.queue.exception.PayloadTooLargeException;
import com.github.h3yow.reliablemessagequeuejava.queue.exception.QueueException;
import com.github.h3yow.reliablemessagequeuejava.queue.exception.TopicCapacityExceededException;
import com.github.h3yow.reliablemessagequeuejava.queue.exception.TopicNotFoundException;
import com.github.h3yow.reliablemessagequeuejava.queue.model.DeadLetter;
import com.github.h3yow.reliablemessagequeuejava.queue.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.queue.model.PublishResult;
import com.github.h3yow.reliablemessagequeuejava.queue.model.ReplayResult;
import com.github.h3yow.reliablemessagequeuejava.queue.model.StoredMessage;
import com.github.h3yow.reliablemessagequeuejava.queue.model.TopicConfig;
import com.github.h3yow.reliablemessagequeuejava.queue.state.BrokerState;
import com.github.h3yow.reliablemessagequeuejava.queue.state.GroupState;
import com.github.h3yow.reliablemessagequeuejava.queue.state.Lease;
import com.github.h3yow.reliablemessagequeuejava.queue.state.NackState;
import com.github.h3yow.reliablemessagequeuejava.queue.storage.BackoffPolicy;
import com.github.h3yow.reliablemessagequeuejava.queue.storage.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 本地可靠消息队列。
 *
 * <p>所有公开方法都在单把锁下串行执行并在变更后立即把全量状态持久化到本地 JSON 快照，
 * 因此可以被多线程并发调用，且应用重启后状态不丢。
 * <ul>
 *   <li>同一分区键哈希到同一分区，分区内按位点严格有序；</li>
 *   <li>不同消费组各自维护位点、租约、退避与死信；</li>
 *   <li>未确认消息在租约超时（或重启后）会被重新投递；</li>
 *   <li>消费失败按指数退避重试，超过上限进入死信；死信可按原内容重放为一条新消息。</li>
 * </ul>
 */
public class ReliableMessageQueue implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ReliableMessageQueue.class);

    private final StateStore store;
    private final BrokerState state;
    private final long leaseMillis;
    private final long backoffBaseMillis;
    private final long backoffMaxMillis;
    private final int maxAttempts;

    private ScheduledExecutorService reaper;
    private volatile boolean started;

    public ReliableMessageQueue(Path dataFile, long leaseMillis, long backoffBaseMillis,
                                long backoffMaxMillis, int maxAttempts) {
        this(new StateStore(dataFile), leaseMillis, backoffBaseMillis, backoffMaxMillis, maxAttempts);
    }

    public ReliableMessageQueue(StateStore store, long leaseMillis, long backoffBaseMillis,
                                long backoffMaxMillis, int maxAttempts) {
        this.store = store;
        this.state = store.load();
        this.leaseMillis = leaseMillis;
        this.backoffBaseMillis = backoffBaseMillis;
        this.backoffMaxMillis = backoffMaxMillis;
        this.maxAttempts = maxAttempts;
    }

    /** 启动后台租约回收器：把超时未确认的消息重新置为可投递。 */
    public synchronized void start() {
        if (started) {
            return;
        }
        // 应用重启意味着旧的消费者会话全部结束：持久化下来的在途租约立即失效，
        // 对应未确认消息马上可被再次消费（不必等待租约超时）。
        int recovered = 0;
        for (GroupState gs : state.groups.values()) {
            recovered += gs.leasesByKey.size();
            gs.leasesByKey.clear();
        }
        if (recovered > 0) {
            persist();
            log.info("应用重启，{} 条未确认消息的旧租约已失效并可重新投递", recovered);
        }
        started = true;
        reaper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rmq-lease-reaper");
            t.setDaemon(true);
            return t;
        });
        long period = Math.max(100L, Math.min(leaseMillis / 3, 1_000L));
        reaper.scheduleWithFixedDelay(() -> {
            try {
                int n = requeueExpiredLeases(System.currentTimeMillis());
                if (n > 0) {
                    log.info("回收超时租约并重新投递 count={}", n);
                }
            } catch (RuntimeException e) {
                log.warn("租约回收失败", e);
            }
        }, period, period, TimeUnit.MILLISECONDS);
        log.info("可靠消息队列已启动 leaseMillis={} backoff={}/{} maxAttempts={}",
                leaseMillis, backoffBaseMillis, backoffMaxMillis, maxAttempts);
    }

    // ---------------- 主题 ----------------

    public synchronized void createTopic(TopicConfig config) {
        validateTopicName(config.name());
        if (config.partitionCount() <= 0) {
            throw new InvalidTopicException("分区数必须大于 0: " + config.partitionCount());
        }
        if (config.capacity() <= 0) {
            throw new InvalidTopicException("主题容量必须大于 0: " + config.capacity());
        }
        if (config.maxPayloadBytes() <= 0) {
            throw new InvalidTopicException("最大负载字节数必须大于 0");
        }
        if (state.topics.putIfAbsent(config.name(), config) != null) {
            throw new InvalidTopicException("主题已存在: " + config.name());
        }
        state.idempotencyKeys.put(config.name(), new java.util.HashSet<>());
        state.partitionMeta.put(config.name(), new long[config.partitionCount()]);
        state.topicSequences.put(config.name(), 0L);
        persist();
        log.info("创建主题 topic={} partitions={} capacity={} maxPayloadBytes={}",
                config.name(), config.partitionCount(), config.capacity(), config.maxPayloadBytes());
    }

    private void validateTopicName(String name) {
        if (name == null || name.isBlank()) {
            throw new InvalidTopicException("主题名不能为空");
        }
        if (name.length() > 128 || !name.matches("[A-Za-z0-9._-]{1,128}")) {
            throw new InvalidTopicException("非法主题名(只允许字母数字._-且不超过128字符): " + name);
        }
    }

    private TopicConfig requireTopic(String topic) {
        validateTopicName(topic);
        TopicConfig config = state.topics.get(topic);
        if (config == null) {
            throw new TopicNotFoundException("主题不存在: " + topic);
        }
        return config;
    }

    // ---------------- 发布 ----------------

    public synchronized PublishResult publish(String topic, String partitionKey,
                                              String idempotencyKey, byte[] payload) {
        TopicConfig config = requireTopic(topic);
        if (partitionKey == null || partitionKey.isEmpty()) {
            throw new InvalidTopicException("分区键不能为空");
        }
        byte[] body = payload == null ? new byte[0] : payload;
        if (body.length > config.maxPayloadBytes()) {
            throw new PayloadTooLargeException(
                    "负载超长: " + body.length + " > " + config.maxPayloadBytes()
                            + " topic=" + topic + " key=" + partitionKey);
        }
        if (idempotencyKey != null && !idempotencyKey.isEmpty()) {
            boolean added = state.idempotencyKeys.get(topic).add(idempotencyKey);
            if (!added) {
                throw new DuplicateIdempotencyKeyException(
                        "重复的幂等键: topic=" + topic + " idempotencyKey=" + idempotencyKey);
            }
        }
        if (countPendingMessages(topic) >= config.capacity()) {
            throw new TopicCapacityExceededException(
                    "主题容量已满: topic=" + topic + " capacity=" + config.capacity());
        }

        int partition = Math.floorMod(partitionKey.hashCode(), config.partitionCount());
        long[] meta = state.partitionMeta.get(topic);
        long offset = meta[partition]++;
        long seq = state.topicSequences.merge(topic, 1L, Long::sum) - 1;
        StoredMessage msg = new StoredMessage(
                UUID.randomUUID().toString(), topic, partition, offset, seq,
                partitionKey, idempotencyKey, body, System.currentTimeMillis(), null);
        state.messages.add(msg);
        persist();
        log.info("发布消息 topic={} key={} partition={} seq={} offset={} bytes={}",
                topic, partitionKey, partition, seq, offset, body.length);
        return new PublishResult(msg.id(), partition, offset, seq, false);
    }

    /**
     * 容量口径（可预期）：没有任何消费组时，主题内所有消息都占位；存在消费组时，
     * 只要还有任意一个消费组未解决（未确认且未进入该死信）该消息，它就占位。
     */
    private int countPendingMessages(String topic) {
        int count = 0;
        for (StoredMessage m : state.messages) {
            if (m.topic().equals(topic) && isMessageOccupyingCapacity(m)) {
                count++;
            }
        }
        return count;
    }

    private boolean isMessageOccupyingCapacity(StoredMessage m) {
        boolean anyGroupForTopic = false;
        for (GroupState gs : state.groups.values()) {
            if (!gs.topic.equals(m.topic())) {
                continue;
            }
            anyGroupForTopic = true;
            if (!gs.resolvedKeys.contains(m.id()) && !isDead(gs, m.id())) {
                return true;
            }
        }
        return !anyGroupForTopic;
    }

    private boolean isDead(GroupState gs, String messageId) {
        for (DeadLetter d : state.deadLetters) {
            if (d.group().equals(gs.group) && d.topic().equals(gs.topic)
                    && d.originalMessageId().equals(messageId)) {
                return true;
            }
        }
        return false;
    }

    // ---------------- 消费 ----------------

    public synchronized List<Delivery> poll(String group, String topic, int maxMessages) {
        TopicConfig config = requireTopic(topic);
        if (group == null || group.isBlank()) {
            throw new InvalidTopicException("消费组名不能为空");
        }
        int limit = Math.min(Math.max(maxMessages, 1), 10_000);
        long now = System.currentTimeMillis();
        requeueExpiredLeases(now);

        GroupState gs = getOrCreateGroup(group, topic, config.partitionCount());
        List<Delivery> deliveries = new ArrayList<>();
        // 轮询各分区起始位置，避免某个繁忙分区长期独占配额。
        int start = (int) (now % config.partitionCount());
        for (int i = 0; i < config.partitionCount() && deliveries.size() < limit; i++) {
            int partition = (start + i) % config.partitionCount();
            long committed = gs.committedOffsets.getOrDefault(partition, -1L);
            List<StoredMessage> candidates = new ArrayList<>();
            for (StoredMessage m : state.messages) {
                if (m.partition() != partition || !m.topic().equals(topic) || m.offset() <= committed) {
                    continue;
                }
                String key = m.id();
                if (gs.resolvedKeys.contains(key) || gs.leasesByKey.containsKey(key)) {
                    continue;
                }
                if (isDead(gs, key)) {
                    continue;
                }
                NackState ns = gs.nackByKey.get(key);
                if (ns != null && ns.nextVisibleAt > now) {
                    continue;
                }
                candidates.add(m);
            }
            candidates.sort(Comparator.comparingLong(StoredMessage::offset));
            for (StoredMessage m : candidates) {
                if (deliveries.size() >= limit) {
                    break;
                }
                NackState ns = gs.nackByKey.get(m.id());
                int attempt = ns == null ? 1 : ns.attempts + 1;
                Lease lease = new Lease();
                lease.token = UUID.randomUUID().toString();
                lease.group = group;
                lease.topic = topic;
                lease.partition = partition;
                lease.offset = m.offset();
                lease.messageId = m.id();
                lease.attempt = attempt;
                lease.leasedAt = now;
                lease.expiresAt = now + leaseMillis;
                gs.leasesByKey.put(m.id(), lease);
                deliveries.add(new Delivery(topic, group, partition, m.offset(), m.seq(),
                        m.partitionKey(), m.id(), m.payload(), attempt, lease.token));
                log.info("投递消息 topic={} key={} partition={} seq={} offset={} attempt={} group={}",
                        topic, m.partitionKey(), partition, m.seq(), m.offset(), attempt, group);
            }
        }
        if (!deliveries.isEmpty()) {
            persist();
        }
        return deliveries;
    }

    private GroupState getOrCreateGroup(String group, String topic, int partitionCount) {
        String gk = groupKey(group, topic);
        GroupState gs = state.groups.get(gk);
        if (gs == null) {
            gs = new GroupState();
            gs.group = group;
            gs.topic = topic;
            for (int p = 0; p < partitionCount; p++) {
                gs.committedOffsets.put(p, -1L);
            }
            state.groups.put(gk, gs);
            persist();
        }
        return gs;
    }

    public synchronized void ack(Delivery delivery) {
        if (delivery == null) {
            throw new QueueException("INVALID_ACK", "ack 的消息不能为空");
        }
        GroupState gs = getGroup(delivery.group(), delivery.topic());
        Lease lease = gs.leasesByKey.get(delivery.messageId());
        if (lease == null) {
            // 可能是重复确认、位点之前已推进，或属于别的消费组。
            GroupState other = findLeaseAcrossGroups(delivery.messageId());
            if (other != null && !other.group.equals(gs.group)) {
                throw new com.github.h3yow.reliablemessagequeuejava.queue.exception.CrossGroupCommitException(
                        "消息由其他消费组持有，不能跨组确认: messageId=" + delivery.messageId()
                                + " ownerGroup=" + other.group + " ackGroup=" + gs.group);
            }
            throw new DuplicateAckException(
                    "重复确认或租约已失效: topic=" + delivery.topic()
                            + " partition=" + delivery.partition() + " offset=" + delivery.offset()
                            + " group=" + delivery.group());
        }
        if (!lease.token.equals(delivery.leaseToken())) {
            throw new DuplicateAckException(
                    "租约令牌不匹配（可能已超时重投，旧投递不得确认）: messageId=" + delivery.messageId()
                            + " topic=" + delivery.topic() + " offset=" + delivery.offset());
        }
        gs.leasesByKey.remove(delivery.messageId());
        gs.nackByKey.remove(delivery.messageId());
        gs.resolvedKeys.add(delivery.messageId());
        advanceCommitted(gs);
        persist();
        log.info("确认消息 topic={} key={} partition={} seq={} offset={} attempt={} group={} committedOffset={}",
                delivery.topic(), delivery.partitionKey(), delivery.partition(), delivery.seq(),
                delivery.offset(), delivery.attempt(), delivery.group(),
                gs.committedOffsets.get(delivery.partition()));
    }

    public synchronized void nack(Delivery delivery, String error) {
        if (delivery == null) {
            throw new QueueException("INVALID_NACK", "nack 的消息不能为空");
        }
        GroupState gs = getGroup(delivery.group(), delivery.topic());
        Lease lease = gs.leasesByKey.get(delivery.messageId());
        if (lease == null || !lease.token.equals(delivery.leaseToken())) {
            throw new DuplicateAckException(
                    "租约不存在或已失效，不能 nack: messageId=" + delivery.messageId()
                            + " topic=" + delivery.topic() + " offset=" + delivery.offset());
        }
        gs.leasesByKey.remove(delivery.messageId());

        NackState ns = gs.nackByKey.getOrDefault(delivery.messageId(), new NackState());
        ns.attempts += 1;
        ns.lastError = error;
        long now = System.currentTimeMillis();

        if (ns.attempts >= maxAttempts) {
            gs.nackByKey.remove(delivery.messageId());
            DeadLetter dead = new DeadLetter(
                    UUID.randomUUID().toString(), delivery.group(), delivery.topic(),
                    delivery.partition(), delivery.offset(), delivery.seq(), delivery.partitionKey(),
                    delivery.messageId(), delivery.payload(), ns.attempts, error, now,
                    false, null, null);
            state.deadLetters.add(dead);
            gs.resolvedKeys.add(delivery.messageId());
            advanceCommitted(gs);
            persist();
            log.warn("消息进入死信 topic={} key={} partition={} seq={} offset={} attempts={} group={} dlqId={}",
                    delivery.topic(), delivery.partitionKey(), delivery.partition(), delivery.seq(),
                    delivery.offset(), ns.attempts, delivery.group(), dead.deadLetterId());
            return;
        }

        long delay = BackoffPolicy.nextDelayMillis(ns.attempts, backoffBaseMillis, backoffMaxMillis);
        ns.nextVisibleAt = now + delay;
        gs.nackByKey.put(delivery.messageId(), ns);
        persist();
        log.warn("消费失败，等待退避重试 topic={} key={} partition={} seq={} offset={} attempts={} group={} backoffMs={} error={}",
                delivery.topic(), delivery.partitionKey(), delivery.partition(), delivery.seq(),
                delivery.offset(), ns.attempts, delivery.group(), delay, error);
    }

    /**
     * 手动提交位点。位点必须单调递增；只允许提交到“连续已解决”的位点。
     */
    public synchronized void commitSync(String group, String topic, Map<Integer, Long> offsets) {
        requireTopic(topic);
        GroupState gs = getGroup(group, topic);
        for (Map.Entry<Integer, Long> e : offsets.entrySet()) {
            int partition = e.getKey();
            long target = e.getValue();
            long current = gs.committedOffsets.getOrDefault(partition, -1L);
            if (target < current) {
                throw new OffsetRollbackException(
                        "位点回退被拒绝: topic=" + topic + " group=" + group
                                + " partition=" + partition + " current=" + current + " target=" + target);
            }
            if (target == current) {
                // 重复提交相同位点属于无操作，但为可区分地拒绝显式重复提交，这里明确拒绝。
                throw new DuplicateAckException(
                        "重复提交相同位点: topic=" + topic + " group=" + group
                                + " partition=" + partition + " offset=" + target);
            }
            long contiguous = highestContiguousResolved(gs, topic, partition);
            if (target > contiguous) {
                throw new OffsetRollbackException(
                        "不能提交未连续确认的位点（存在缺口）: topic=" + topic + " group=" + group
                                + " partition=" + partition + " target=" + target
                                + " maxContiguous=" + contiguous);
            }
            gs.committedOffsets.put(partition, target);
        }
        persist();
        log.info("手动提交位点 topic={} group={} offsets={}", topic, group, offsets);
    }

    public synchronized long committedOffset(String group, String topic, int partition) {
        GroupState gs = getGroup(group, topic);
        return gs.committedOffsets.getOrDefault(partition, -1L);
    }

    // ---------------- 死信 ----------------

    public synchronized List<DeadLetter> listDeadLetters(String group, String topic) {
        requireTopic(topic);
        List<DeadLetter> result = new ArrayList<>();
        for (DeadLetter d : state.deadLetters) {
            if (d.topic().equals(topic) && (group == null || d.group().equals(group))) {
                result.add(d);
            }
        }
        return List.copyOf(result);
    }

    /**
     * 重放死信：把死信原始内容作为一条<em>新消息</em>追加到同主题同分区键，
     * 旧消息、旧位点、死信记录都保留，因此不破坏已有位点与幂等语义，也不会丢消息。
     */
    public synchronized ReplayResult replayDeadLetter(String deadLetterId, String group) {
        DeadLetter target = null;
        int index = -1;
        for (int i = 0; i < state.deadLetters.size(); i++) {
            DeadLetter d = state.deadLetters.get(i);
            if (d.deadLetterId().equals(deadLetterId)) {
                target = d;
                index = i;
                break;
            }
        }
        if (target == null) {
            throw new QueueException("DEAD_LETTER_NOT_FOUND", "死信不存在: " + deadLetterId);
        }
        if (group != null && !group.equals(target.group())) {
            throw new com.github.h3yow.reliablemessagequeuejava.queue.exception.CrossGroupCommitException(
                    "死信属于其他消费组，不能跨组重放: owner=" + target.group() + " request=" + group);
        }
        if (target.replayed()) {
            throw new DuplicateIdempotencyKeyException(
                    "死信已重放，不能重复重放: " + deadLetterId);
        }
        // 重放产生全新消息（新 id、新位点、新幂等键作用域），保证不与历史投递混淆。
        PublishResult pr = publish(target.topic(), target.partitionKey(),
                "replay:" + deadLetterId, target.payload());
        StoredMessage replayedMsg = state.messages.get(state.messages.size() - 1);
        DeadLetter updated = new DeadLetter(target.deadLetterId(), target.group(), target.topic(),
                target.partition(), target.originalOffset(), target.seq(), target.partitionKey(),
                target.originalMessageId(), target.payload(), target.attempts(), target.lastError(),
                target.deadAt(), true, pr.offset(), replayedMsg.id());
        state.deadLetters.set(index, updated);
        persist();
        log.info("重放死信 dlqId={} topic={} key={} 原offset={} -> 新seq={} 新offset={} group={}",
                deadLetterId, target.topic(), target.partitionKey(), target.originalOffset(),
                pr.seq(), pr.offset(), target.group());
        return new ReplayResult(updated, pr);
    }

    // ---------------- 租约回收 ----------------

    /**
     * 把所有已到期的在途租约重新置为可投递（消费者崩溃/超时/重启后的重新投递入口）。
     * 超时本身不增加失败次数，失败次数仅由显式 nack 增加；下一次 poll 的 attempt 会反映当前失败次数。
     */
    public synchronized int requeueExpiredLeases(long nowMillis) {
        int count = 0;
        for (GroupState gs : state.groups.values()) {
            List<String> expired = new ArrayList<>();
            for (Lease lease : gs.leasesByKey.values()) {
                if (lease.expiresAt <= nowMillis) {
                    expired.add(lease.messageId);
                }
            }
            for (String messageId : expired) {
                Lease lease = gs.leasesByKey.remove(messageId);
                if (lease != null) {
                    count++;
                    log.info("租约超时，重新置为可投递 topic={} partition={} offset={} group={} attemptWas={}",
                            lease.topic, lease.partition, lease.offset, lease.group, lease.attempt);
                }
            }
        }
        if (count > 0) {
            persist();
        }
        return count;
    }

    // ---------------- 内部辅助 ----------------

    private GroupState getGroup(String group, String topic) {
        GroupState gs = state.groups.get(groupKey(group, topic));
        if (gs == null) {
            throw new TopicNotFoundException(
                    "消费组尚未在该主题建立消费: group=" + group + " topic=" + topic
                            + "（请先 poll 再确认/提交）");
        }
        return gs;
    }

    private GroupState findLeaseAcrossGroups(String messageId) {
        for (GroupState gs : state.groups.values()) {
            if (gs.leasesByKey.containsKey(messageId)) {
                return gs;
            }
        }
        return null;
    }

    private void advanceCommitted(GroupState gs) {
        for (int partition : new ArrayList<>(gs.committedOffsets.keySet())) {
            long contiguous = highestContiguousResolved(gs, gs.topic, partition);
            if (contiguous > gs.committedOffsets.getOrDefault(partition, -1L)) {
                gs.committedOffsets.put(partition, contiguous);
            }
        }
    }

    private long highestContiguousResolved(GroupState gs, String topic, int partition) {
        long committed = gs.committedOffsets.getOrDefault(partition, -1L);
        long candidate = committed;
        while (true) {
            long next = candidate + 1;
            StoredMessage m = findMessage(topic, partition, next);
            if (m == null) {
                break;
            }
            boolean resolved = gs.resolvedKeys.contains(m.id()) || isDead(gs, m.id());
            if (!resolved) {
                break;
            }
            candidate = next;
        }
        return candidate;
    }

    private StoredMessage findMessage(String topic, int partition, long offset) {
        for (StoredMessage m : state.messages) {
            if (m.topic().equals(topic) && m.partition() == partition && m.offset() == offset) {
                return m;
            }
        }
        return null;
    }

    private static String groupKey(String group, String topic) {
        return group + '\u0000' + topic;
    }

    private void persist() {
        store.save(state);
    }

    @Override
    public synchronized void close() {
        if (reaper != null) {
            reaper.shutdownNow();
            reaper = null;
        }
        started = false;
        log.info("可靠消息队列已关闭");
    }

    /** 便于本地演示与测试：以 UTF-8 字符串作为负载发布。 */
    public PublishResult publishText(String topic, String partitionKey, String idempotencyKey, String text) {
        return publish(topic, partitionKey, idempotencyKey,
                text == null ? new byte[0] : text.getBytes(StandardCharsets.UTF_8));
    }
}
