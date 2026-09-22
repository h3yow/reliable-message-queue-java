package com.github.h3yow.reliablemessagequeuejava.core;

import com.github.h3yow.reliablemessagequeuejava.config.QueueProperties;
import com.github.h3yow.reliablemessagequeuejava.exception.CrossGroupCommitException;
import com.github.h3yow.reliablemessagequeuejava.exception.DuplicateAckException;
import com.github.h3yow.reliablemessagequeuejava.exception.DuplicateMessageException;
import com.github.h3yow.reliablemessagequeuejava.exception.InvalidTopicException;
import com.github.h3yow.reliablemessagequeuejava.exception.LeaseExpiredException;
import com.github.h3yow.reliablemessagequeuejava.exception.OffsetRegressionException;
import com.github.h3yow.reliablemessagequeuejava.exception.PayloadTooLargeException;
import com.github.h3yow.reliablemessagequeuejava.exception.TopicCapacityExceededException;
import com.github.h3yow.reliablemessagequeuejava.model.DeadLetterEntry;
import com.github.h3yow.reliablemessagequeuejava.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.model.InFlightLease;
import com.github.h3yow.reliablemessagequeuejava.model.Message;
import com.github.h3yow.reliablemessagequeuejava.model.PublishReceipt;
import com.github.h3yow.reliablemessagequeuejava.model.TopicConfig;
import com.github.h3yow.reliablemessagequeuejava.storage.PersistentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

/**
 * 可靠消息队列门面。
 *
 * <h3>投递语义</h3>
 * <ul>
 *   <li>至少一次（at-least-once）：租约超时 / nack / 进程重启都会重新投递，由消费端借助幂等键去重。</li>
 *   <li>同分区键有序：分区由 {@code hash(partitionKey)} 决定，每个（组,分区）的未确认窗口为 1，
 *       前一条未确认（或未退避结束）时不会交付同分区后一条。</li>
 *   <li>消费组独立：每个 group 独立维护位点、租约与死信。</li>
 * </ul>
 *
 * <h3>位点</h3>
 * 位点语义为"最后已确认消息的序号"，初始 -1，只能严格 +1 前进；回退、跨组提交、重复确认分别抛出
 * {@link OffsetRegressionException} / {@link CrossGroupCommitException} / {@link DuplicateAckException}。
 *
 * <h3>重试与死信</h3>
 * nack 后按指数退避（base * 2^(attempt-1)，上限 backoffMax）重试，attempt 达到 maxAttempts 后进入死信，
 * 位点随之推进，不阻塞同分区后续消息，也不阻塞其他分区。死信重放以"新消息"追加到分区尾部。
 */
public class MessageQueue implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MessageQueue.class);
    private static final Pattern TOPIC_PATTERN = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,63}");
    private static final Pattern GROUP_PATTERN = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,63}");

    private final QueueProperties properties;
    private final PersistentStore store;
    private final LongSupplier clock;

    /** 每个 (topic, partition) 发布串行化，保证序号分配原子 */
    private final Map<String, Object> publishLocks = new ConcurrentHashMap<>();
    /** 每个 (group, topic) poll 串行化 */
    private final Map<String, Object> pollLocks = new ConcurrentHashMap<>();
    /** 每个 (group, topic) 轮询分区的起始游标 */
    private final Map<String, Integer> pollCursors = new ConcurrentHashMap<>();
    /** 进程内活跃交付：deliveryId -> lease（重启后清空，仅持久化租约仍可恢复） */
    private final Map<String, InFlightLease> activeDeliveries = new ConcurrentHashMap<>();

    public MessageQueue(QueueProperties properties) {
        this(properties, new PersistentStore(properties.getDataDir()), System::currentTimeMillis);
    }

    public MessageQueue(QueueProperties properties, PersistentStore store) {
        this(properties, store, System::currentTimeMillis);
    }

    public MessageQueue(QueueProperties properties, PersistentStore store, LongSupplier clock) {
        this.properties = properties;
        this.store = store;
        this.clock = clock;
    }

    // ==================== topic ====================

    public void createTopic(String name, int partitions, long capacity, int maxPayloadBytes) {
        createTopic(new TopicConfig(name, partitions, capacity, maxPayloadBytes,
                java.time.Duration.ZERO));
    }

    /** 创建主题；同名同配置幂等成功，同名但配置冲突拒绝。 */
    public void createTopic(TopicConfig config) {
        validateTopicName(config.name());
        if (config.partitionCount() <= 0) {
            throw new InvalidTopicException("分区数必须大于 0: " + config.partitionCount());
        }
        if (config.capacity() <= 0 || config.maxPayloadBytes() <= 0) {
            throw new InvalidTopicException("容量与负载上限必须大于 0");
        }
        Optional<TopicConfig> existing = store.loadTopicConfig(config.name());
        if (existing.isPresent()) {
            if (!existing.get().equals(config)) {
                throw new InvalidTopicException("主题已存在且配置不一致: " + config.name());
            }
            return;
        }
        store.saveTopicConfig(config);
        log.info("topic created: name={} partitions={} capacity={} maxPayloadBytes={}",
                config.name(), config.partitionCount(), config.capacity(), config.maxPayloadBytes());
    }

    public Optional<TopicConfig> describeTopic(String name) {
        return store.loadTopicConfig(name);
    }

    // ==================== publish ====================

    public PublishReceipt publish(String topic, String partitionKey, String idempotencyKey, byte[] payload) {
        TopicConfig config = requireTopic(topic);
        if (partitionKey == null || partitionKey.isEmpty()) {
            throw new InvalidTopicException("partitionKey 不能为空");
        }
        byte[] body = payload == null ? new byte[0] : payload;
        if (body.length > config.maxPayloadBytes()) {
            throw new PayloadTooLargeException(String.format(
                    "负载超长: topic=%s size=%d max=%d", topic, body.length, config.maxPayloadBytes()));
        }
        int partition = partition(partitionKey, config.partitionCount());

        // 主题级串行化：容量检查 / 幂等判重 / 序号分配作为一个原子单元，
        // 杜绝跨分区并发写入同时通过容量或幂等检查。
        synchronized (publishLock(topic)) {
            if (idempotencyKey != null && !idempotencyKey.isEmpty()) {
                Optional<PersistentStore.IdempotentRecord> dup =
                        store.findIdempotentMessage(topic, idempotencyKey);
                if (dup.isPresent()) {
                    PersistentStore.IdempotentRecord r = dup.get();
                    throw new DuplicateMessageException(String.format(
                            "幂等键重复: topic=%s key=%s existingMessageId=%s",
                            topic, idempotencyKey, r.messageId()));
                }
            }

            long total = 0L;
            for (int p = 0; p < config.partitionCount(); p++) {
                total += store.messageCount(topic, p);
            }
            if (total >= config.capacity()) {
                throw new TopicCapacityExceededException(String.format(
                        "主题容量耗尽: topic=%s size=%d capacity=%d", topic, total, config.capacity()));
            }

            long seq = store.nextSeq(topic, partition);
            String id = UUID.randomUUID().toString();
            Message message = new Message(id, topic, partitionKey, idempotencyKey, body,
                    partition, seq, clock.getAsLong());
            long storedSeq = store.appendMessage(message);
            if (idempotencyKey != null && !idempotencyKey.isEmpty()) {
                store.saveIdempotencyKey(topic, idempotencyKey, id, partition, storedSeq);
            }
            log.info("publish topic={} partition={} key={} idempotencyKey={} seq={} attempt=- offset={}",
                    topic, partition, partitionKey, idempotencyKey, storedSeq, storedSeq);
            return new PublishReceipt(id, topic, partition, storedSeq);
        }
    }

    // ==================== poll ====================

    /**
     * 拉取一条可交付消息。每个（组, 分区）未确认窗口为 1，保证同分区相对顺序。
     * 租约到期的未确认消息会被重新投递（attempt+1），其旧 deliveryId 立即失效。
     */
    public Optional<Delivery> poll(String group, String topic) {
        validateGroupName(group);
        TopicConfig config = requireTopic(topic);
        synchronized (pollLock(group, topic)) {
            int start = pollCursors.merge(group + "|" + topic, 1, Integer::sum) - 1;
            for (int i = 0; i < config.partitionCount(); i++) {
                int partition = (start + i) % config.partitionCount();
                long committed = store.loadCommittedOffset(group, topic, partition);
                long now = clock.getAsLong();

                InFlightLease active = currentLease(group, topic, partition);
                long offsetToDeliver;
                InFlightLease existing = active;
                if (active != null) {
                    if (now < active.leasedUntil() || now < active.backoffUntil()) {
                        continue; // 窗口占用中（未超时且未到退避时间）
                    }
                    // 租约超时：重新投递同一条
                    offsetToDeliver = active.offset();
                } else {
                    offsetToDeliver = committed + 1;
                }

                Message message = store.readMessageAt(topic, partition, offsetToDeliver);
                if (message == null) {
                    continue; // 该分区暂无新消息
                }

                int attempt = existing == null ? 1 : existing.attempt() + 1;
                String deliveryId = UUID.randomUUID().toString();
                long leasedAt = now;
                long leasedUntil = now + properties.getVisibilityTimeout().toMillis();
                long backoffUntil = 0L;
                InFlightLease lease = new InFlightLease(deliveryId, group, topic, partition,
                        offsetToDeliver, attempt, leasedAt, leasedUntil, backoffUntil,
                        existing == null ? null : existing.lastError());
                store.saveLease(lease);
                if (existing != null) {
                    // 旧交付被新交付取代：逻辑删除旧租约，保证每个（组,分区）至多一条活动租约
                    store.removeLease(group, existing.deliveryId());
                    store.saveSupersededDelivery(group, existing.deliveryId(), deliveryId);
                    activeDeliveries.remove(existing.deliveryId());
                }
                activeDeliveries.put(deliveryId, lease);
                log.info("deliver topic={} key={} seq={} partition={} group={} deliveryId={} attempt={} offset={} committed={}",
                        topic, message.partitionKey(), message.seq(), partition, group,
                        deliveryId, attempt, offsetToDeliver, committed);
                return Optional.of(new Delivery(deliveryId, group, message, partition,
                        offsetToDeliver, attempt, leasedAt, leasedUntil));
            }
            return Optional.empty();
        }
    }

    // ==================== ack ====================

    public void ack(String group, String deliveryId) {
        InFlightLease lease = resolveLeaseForCommit(group, deliveryId);
        long committed = store.loadCommittedOffset(group, lease.topic(), lease.partition());
        long expected = committed + 1;
        if (lease.offset() < expected) {
            throw new OffsetRegressionException(String.format(
                    "位点回退被拒绝: group=%s topic=%s partition=%d current=%d submitted=%d",
                    group, lease.topic(), lease.partition(), committed, lease.offset()));
        }
        if (lease.offset() > expected) {
            throw new OffsetRegressionException(String.format(
                    "位点跳跃被拒绝: group=%s topic=%s partition=%d current=%d submitted=%d (前序消息未确认)",
                    group, lease.topic(), lease.partition(), committed, lease.offset()));
        }
        store.commitOffset(group, lease.topic(), lease.partition(), lease.offset());
        store.removeLease(group, deliveryId);
        activeDeliveries.remove(deliveryId);
        log.info("ack group={} topic={} partition={} deliveryId={} attempt={} offset={}",
                group, lease.topic(), lease.partition(), deliveryId, lease.attempt(), lease.offset());
    }

    // ==================== nack / retry / dlq ====================

    /**
     * 否认消费。返回下一次尝试序号（nack 后本次 attempt）；若已达上限进入死信，返回 -1。
     */
    public int nack(String group, String deliveryId, String error) {
        InFlightLease lease = resolveLeaseForCommit(group, deliveryId);
        long now = clock.getAsLong();
        int attempt = lease.attempt();

        if (attempt >= properties.getMaxAttempts()) {
            DeadLetterEntry entry = new DeadLetterEntry(
                    UUID.randomUUID().toString(), group,
                    store.readMessageAt(lease.topic(), lease.partition(), lease.offset()),
                    lease.partition(), lease.offset(), attempt,
                    error == null ? "unknown" : error, now);
            store.appendDeadLetter(entry);
            // 毒消息跳过：位点推进 +1，窗口释放，后续消息不再被阻塞
            store.commitOffset(group, lease.topic(), lease.partition(), lease.offset());
            store.removeLease(group, deliveryId);
            activeDeliveries.remove(deliveryId);
            log.warn("dead-letter group={} topic={} partition={} seq={} key={} attempts={} offset={} dlqId={} error={}",
                    group, lease.topic(), lease.partition(), lease.offset(),
                    entry.message().partitionKey(), attempt, lease.offset(),
                    entry.deadLetterId(), error);
            return -1;
        }

        long backoff = backoffMillis(attempt);
        // nack 后本次交付立即失效（leasedUntil=now），是否可再次交付仅由退避时刻决定
        InFlightLease updated = new InFlightLease(deliveryId, group, lease.topic(), lease.partition(),
                lease.offset(), attempt, lease.leasedAt(), now,
                now + backoff, error);
        store.saveLease(updated);
        activeDeliveries.put(deliveryId, updated);
        log.warn("nack group={} topic={} partition={} key={} seq={} deliveryId={} attempt={} offset={} backoffMs={} error={}",
                group, lease.topic(), lease.partition(),
                store.readMessageAt(lease.topic(), lease.partition(), lease.offset()).partitionKey(),
                lease.offset(), deliveryId, attempt, lease.offset(), backoff, error);
        return attempt;
    }

    /** 指数退避：base * 2^(attempt-1)，封顶 backoffMax。 */
    long backoffMillis(int attempt) {
        long base = properties.getBackoffBase().toMillis();
        long max = properties.getBackoffMax().toMillis();
        if (attempt <= 1) {
            return Math.min(base, max);
        }
        long raw = base * (1L << Math.min(attempt - 1, 40));
        return raw <= 0 || raw > max ? max : raw;
    }

    // ==================== offsets / dlq ====================

    public long committedOffset(String group, String topic, int partition) {
        return store.loadCommittedOffset(group, topic, partition);
    }

    /**
     * 显式提交位点（管理型接口，不依赖具体交付）。
     * 只允许提交为"已存在的消息序号"且严格大于当前位点，回退与跳跃都以
     * {@link OffsetRegressionException} 拒绝。
     */
    public void commitOffset(String group, String topic, int partition, long offset) {
        validateGroupName(group);
        TopicConfig config = requireTopic(topic);
        if (partition < 0 || partition >= config.partitionCount()) {
            throw new InvalidTopicException("分区越界: " + partition);
        }
        long current = store.loadCommittedOffset(group, topic, partition);
        if (offset <= current) {
            throw new OffsetRegressionException(String.format(
                    "位点回退被拒绝: group=%s topic=%s partition=%d current=%d submitted=%d",
                    group, topic, partition, current, offset));
        }
        if (store.readMessageAt(topic, partition, offset) == null) {
            throw new OffsetRegressionException(String.format(
                    "位点跳跃被拒绝: group=%s topic=%s partition=%d current=%d submitted=%d (目标消息不存在)",
                    group, topic, partition, current, offset));
        }
        store.commitOffset(group, topic, partition, offset);
        log.info("commit group={} topic={} partition={} current={} offset={}",
                group, topic, partition, current, offset);
    }

    public List<DeadLetterEntry> deadLetters(String group, String topic) {
        return store.loadDeadLetters(group, topic);
    }

    public List<DeadLetterEntry> allDeadLetters(String group) {
        return store.loadAllDeadLetters(group);
    }

    /**
     * 死信重放：把死信消息作为一条"新消息"追加到原分区尾部（位点从队列尾部继续），
     * 不修改、不删除任何历史位点；幂等键使用确定性键，同一死信重复重放返回同一回执，
     * 不产生第二次可见投递。
     */
    public PublishReceipt replayDeadLetter(String group, String deadLetterId) {
        validateGroupName(group);
        DeadLetterEntry entry = store.findDeadLetter(group, deadLetterId);
        if (entry == null) {
            throw new InvalidTopicException("死信条目不存在或已重放: group=" + group + " id=" + deadLetterId);
        }
        String deterministicKey = "dlq-replay:" + deadLetterId;
        Optional<PersistentStore.IdempotentRecord> existing =
                store.findIdempotentMessage(entry.message().topic(), deterministicKey);
        PublishReceipt receipt;
        if (existing.isPresent()) {
            PersistentStore.IdempotentRecord r = existing.get();
            receipt = new PublishReceipt(r.messageId(), entry.message().topic(),
                    r.partition(), r.offset());
        } else {
            Message origin = entry.message();
            try {
                receipt = publish(origin.topic(), origin.partitionKey(), deterministicKey, origin.payload());
            } catch (TopicCapacityExceededException e) {
                throw new TopicCapacityExceededException(
                        "死信重放失败（主题容量已满，请先扩容或清理）: dlqId=" + deadLetterId, e);
            }
            log.info("dlq replay group={} dlqId={} topic={} newSeq={}",
                    group, deadLetterId, origin.topic(), receipt.offset());
        }
        if (!store.markDeadLetterReplayed(group, deadLetterId)) {
            // 极端并发：另一线程已重放。因幂等键确定，返回的仍是同一条消息，无副作用
            log.debug("dlq entry already marked replayed: group={} dlqId={}", group, deadLetterId);
        }
        return receipt;
    }

    /**
     * 回收租约超时的未确认消息。重新投递是惰性发生的（下次 poll 同分区时），
     * 本方法仅扫描并返回已超时的租约数量，用于定时任务可观测性与重启恢复。
     */
    public int reclaimExpiredLeases() {
        long now = clock.getAsLong();
        int expired = 0;
        for (String group : store.listGroups()) {
            for (InFlightLease lease : store.loadLeases(group)) {
                if (now >= lease.leasedUntil()) {
                    expired++;
                    if (log.isDebugEnabled()) {
                        log.debug("expired lease reclaimable: group={} topic={} partition={} offset={} attempt={} deliveryId={}",
                                group, lease.topic(), lease.partition(), lease.offset(),
                                lease.attempt(), lease.deliveryId());
                    }
                }
            }
        }
        if (expired > 0) {
            log.info("reclaim scan found {} expired lease(s)", expired);
        }
        return expired;
    }

    @Override
    public void close() {
        store.close();
        log.info("MessageQueue closed");
    }

    // ==================== internals ====================

    /** 确认/否认前定位租约，并按可区分原因拒绝非法提交。 */
    private InFlightLease resolveLeaseForCommit(String group, String deliveryId) {
        validateGroupName(group);
        InFlightLease lease = store.findLease(group, deliveryId);
        if (lease != null) {
            return lease;
        }
        // 是否属于其他消费组（优先于失效判断，保证跨组原因可区分）？
        for (String other : store.listGroups()) {
            if (!other.equals(group)) {
                InFlightLease foreign = store.findLease(other, deliveryId);
                if (foreign != null) {
                    throw new CrossGroupCommitException(String.format(
                            "跨消费组提交被拒绝: deliveryId=%s ownerGroup=%s submittedGroup=%s",
                            deliveryId, other, group));
                }
            }
        }
        // 本组曾交付过但已被重投取代 / 本组 superseded 索引命中？
        InFlightLease seen = activeDeliveries.get(deliveryId);
        boolean inThisGroup = seen == null || group.equals(seen.group());
        if ((seen != null && inThisGroup) || store.isSuperseded(group, deliveryId)) {
            throw new LeaseExpiredException(String.format(
                    "租约已失效（消息已重新投递）: group=%s deliveryId=%s", group, deliveryId));
        }
        throw new DuplicateAckException(String.format(
                "重复确认被拒绝: group=%s deliveryId=%s (位点已提交或交付不存在)", group, deliveryId));
    }

    private InFlightLease currentLease(String group, String topic, int partition) {
        for (InFlightLease l : store.loadLeases(group)) {
            if (l.topic().equals(topic) && l.partition() == partition) {
                return l;
            }
        }
        return null;
    }

    private TopicConfig requireTopic(String topic) {
        validateTopicName(topic);
        return store.loadTopicConfig(topic)
                .orElseThrow(() -> new InvalidTopicException("主题不存在: " + topic));
    }

    private static void validateTopicName(String name) {
        if (name == null || !TOPIC_PATTERN.matcher(name).matches()) {
            throw new InvalidTopicException(
                    "非法主题名（字母/数字开头，仅含字母数字._-，长度1-64）: " + name);
        }
    }

    private static void validateGroupName(String name) {
        if (name == null || !GROUP_PATTERN.matcher(name).matches()) {
            throw new InvalidTopicException(
                    "非法消费组名（字母/数字开头，仅含字母数字._-，长度1-64）: " + name);
        }
    }

    /** 分区键 -> 分区。使用稳定 32 位 FNV-1a，避免依赖 String.hashCode 的传播面。 */
    static int partition(String key, int partitions) {
        int hash = 0x811c9dc5;
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        for (byte b : bytes) {
            hash ^= (b & 0xff);
            hash *= 0x01000193;
        }
        return Math.floorMod(hash, partitions);
    }

    private Object publishLock(String topic) {
        return publishLocks.computeIfAbsent("pub#" + topic, k -> new Object());
    }

    private Object pollLock(String group, String topic) {
        return pollLocks.computeIfAbsent("poll#" + group + "#" + topic, k -> new Object());
    }
}
