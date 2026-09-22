package com.github.h3yow.reliablemessagequeuejava.storage;

import com.github.h3yow.reliablemessagequeuejava.model.DeadLetterEntry;
import com.github.h3yow.reliablemessagequeuejava.model.InFlightLease;
import com.github.h3yow.reliablemessagequeuejava.model.Message;
import com.github.h3yow.reliablemessagequeuejava.model.TopicConfig;

import java.util.List;
import java.util.Optional;

/**
 * 基于本地文件的持久化存储。
 *
 * 文件布局（dataDir 下）：
 *  topics/<topic>.json            主题配置
 *  messages/<topic>/p<n>.log      分区消息顺序日志（追加）
 *  groups/<group>/offsets.log     已确认位点（单调）
 *  groups/<group>/leases.log      未确认租约（upsert / 删除）
 *  groups/<group>/dlq.log         死信（追加）
 *  idem/<topic>.idem              幂等键索引
 */
public class PersistentStore {

    public PersistentStore(String dataDir) {
        // TODO
    }

    // ---- topic ----
    public void saveTopicConfig(TopicConfig config) { /* TODO */ }
    public Optional<TopicConfig> loadTopicConfig(String topic) { return Optional.empty(); }
    public List<String> listTopics() { return List.of(); }

    // ---- messages ----
    public long appendMessage(Message message) { return 0L; }
    public Message readMessageAt(String topic, int partition, long offset) { return null; }
    public List<Message> readAllMessages(String topic, int partition) { return List.of(); }
    public long messageCount(String topic, int partition) { return 0L; }
    public long nextSeq(String topic, int partition) { return 0L; }

    // ---- idempotency ----
    public boolean saveIdempotencyKey(String topic, String key, String messageId) { return false; }
    public Optional<String> findIdempotentMessage(String topic, String key) { return Optional.empty(); }

    // ---- group offsets ----
    public long loadCommittedOffset(String group, String topic, int partition) { return -1L; }
    public void commitOffset(String group, String topic, int partition, long offset) { /* TODO */ }

    // ---- leases ----
    public List<InFlightLease> loadLeases(String group) { return List.of(); }
    public void saveLease(InFlightLease lease) { /* TODO */ }
    public void removeLease(String group, String deliveryId) { /* TODO */ }

    // ---- dead letters ----
    public void appendDeadLetter(DeadLetterEntry entry) { /* TODO */ }
    public List<DeadLetterEntry> loadDeadLetters(String group, String topic) { return List.of(); }
    public List<DeadLetterEntry> loadAllDeadLetters(String group) { return List.of(); }
    public boolean markDeadLetterReplayed(String group, String deadLetterId) { return false; }

    // ---- lifecycle ----
    public void close() { /* TODO */ }
}
