package com.github.h3yow.reliablemessagequeuejava.core;

import com.github.h3yow.reliablemessagequeuejava.config.QueueProperties;
import com.github.h3yow.reliablemessagequeuejava.model.DeadLetterEntry;
import com.github.h3yow.reliablemessagequeuejava.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.model.PublishReceipt;
import com.github.h3yow.reliablemessagequeuejava.model.TopicConfig;
import com.github.h3yow.reliablemessagequeuejava.storage.PersistentStore;

import java.util.List;
import java.util.Optional;

/**
 * 可靠消息队列门面：发布 / 拉取 / 确认 / 否认重试 / 死信重放。
 */
public class MessageQueue implements AutoCloseable {

    private final QueueProperties properties;
    private final PersistentStore store;

    public MessageQueue(QueueProperties properties) {
        this.properties = properties;
        this.store = new PersistentStore(properties.getDataDir());
    }

    /** 构造注入存储，便于测试。 */
    public MessageQueue(QueueProperties properties, PersistentStore store) {
        this.properties = properties;
        this.store = store;
    }

    public void createTopic(String name, int partitions, long capacity, int maxPayloadBytes) { /* TODO */ }
    public void createTopic(TopicConfig config) { /* TODO */ }
    public Optional<TopicConfig> describeTopic(String name) { return Optional.empty(); }

    public PublishReceipt publish(String topic, String partitionKey, String idempotencyKey, byte[] payload) {
        return null;
    }

    /** 从任意分区拉取一条可交付消息（每个分区的未确认窗口为 1，保证同分区顺序）。 */
    public Optional<Delivery> poll(String group, String topic) { return Optional.empty(); }

    /** 确认消息，提交单调位点。 */
    public void ack(String group, String deliveryId) { /* TODO */ }

    /** 否认消息：按有上限的退避重试，超过上限进入死信。返回新的尝试次数（进入死信时返回 -1）。 */
    public int nack(String group, String deliveryId, String error) { return 0; }

    public long committedOffset(String group, String topic, int partition) { return 0L; }

    public List<DeadLetterEntry> deadLetters(String group, String topic) { return List.of(); }
    public List<DeadLetterEntry> allDeadLetters(String group) { return List.of(); }

    /** 死信重放：以新消息形式重新发布，不影响原位点与幂等语义。 */
    public PublishReceipt replayDeadLetter(String group, String deadLetterId) { return null; }

    /** 回收租约超时的未确认消息（重启后 / 定时调用）。 */
    public int reclaimExpiredLeases() { return 0; }

    @Override
    public void close() { /* TODO */ }
}
