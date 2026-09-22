package com.github.h3yow.reliablemessagequeuejava.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.reliablemessagequeuejava.exception.MqException;
import com.github.h3yow.reliablemessagequeuejava.model.DeadLetterEntry;
import com.github.h3yow.reliablemessagequeuejava.model.InFlightLease;
import com.github.h3yow.reliablemessagequeuejava.model.Message;
import com.github.h3yow.reliablemessagequeuejava.model.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于本地文件的持久化存储（JSON Lines 追加日志 + 进程内缓存）。
 *
 * <p>每次写入都强制刷盘（{@link FileChannel#force(boolean)}），模拟本地持久化：
 * 进程异常退出后重启，状态可从日志重放恢复。</p>
 *
 * 文件布局（dataDir 下）：
 * <pre>
 *  topics/&lt;topic&gt;.json             主题配置（单 JSON）
 *  messages/&lt;topic&gt;/p&lt;n&gt;.log       分区消息顺序日志（每行一条 JSON）
 *  idem/&lt;topic&gt;.log                幂等键索引（每行：key / messageId / partition / offset）
 *  groups/&lt;group&gt;/offsets.log      已确认位点（每行：topic / partition / offset）
 *  groups/&lt;group&gt;/leases.log       未确认租约（每行一条 upsert，"deleted" 表示删除）
 *  groups/&lt;group&gt;/dlq.log          死信（每行一条，"replayed:&lt;id&gt;" 表示已重放标记）
 * </pre>
 */
public class PersistentStore {

    private static final Logger log = LoggerFactory.getLogger(PersistentStore.class);

    private final Path dataDir;
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();

    // ---- 内存缓存（懒加载，append 与 read 共用） ----
    private final ConcurrentHashMap<String, List<Message>> messageCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Map<String, IdempotentRecord>> idemCache = new ConcurrentHashMap<>();
    /** key = group|topic|partition，仅缓存最终位点（重放 offsets.log 得到） */
    private final ConcurrentHashMap<String, Long> offsetCache = new ConcurrentHashMap<>();
    /** key = group，value: deliveryId -> 最新租约状态（null 值表示已删除） */
    private final ConcurrentHashMap<String, Map<String, InFlightLease>> leaseCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<DlqRecord>> dlqCache = new ConcurrentHashMap<>();

    /** 幂等索引记录。 */
    public record IdempotentRecord(String messageId, int partition, long offset) {
    }

    /** 死信原始记录 + 是否已重放。 */
    public record DlqRecord(DeadLetterEntry entry, boolean replayed) {
    }

    public PersistentStore(String dataDir) {
        this.dataDir = Path.of(dataDir);
        try {
            Files.createDirectories(this.dataDir.resolve("topics"));
            Files.createDirectories(this.dataDir.resolve("messages"));
        } catch (IOException e) {
            throw new UncheckedIOException("初始化数据目录失败: " + dataDir, e);
        }
    }

    private Object lockOf(String key) {
        return locks.computeIfAbsent(key, k -> new Object());
    }

    // ==================== topic ====================

    public void saveTopicConfig(TopicConfig config) {
        Path path = topicFile(config.name());
        synchronized (lockOf(path.toString())) {
            writeJson(path, config);
        }
    }

    public Optional<TopicConfig> loadTopicConfig(String topic) {
        Path path = topicFile(topic);
        synchronized (lockOf(path.toString())) {
            if (!Files.exists(path)) {
                return Optional.empty();
            }
            return Optional.of(readJson(path, TopicConfig.class));
        }
    }

    public List<String> listTopics() {
        Path dir = dataDir.resolve("topics");
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (var stream = Files.list(dir)) {
            return stream.filter(p -> p.toString().endsWith(".json"))
                    .map(p -> p.getFileName().toString().replace(".json", ""))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ==================== messages ====================

    /** 追加一条消息，返回其分区序号。必须由上层保证同分区串行调用。 */
    public long appendMessage(Message message) {
        String cacheKey = message.topic() + "#" + message.partition();
        synchronized (lockOf("messages#" + cacheKey)) {
            List<Message> list = loadMessages(message.topic(), message.partition());
            long seq = list.size();
            Message stored = new Message(message.id(), message.topic(), message.partitionKey(),
                    message.idempotencyKey(), message.payload(), message.partition(), seq, message.publishedAt());
            appendLine(messageLog(message.topic(), message.partition()), stored);
            list.add(stored);
            log.debug("store append topic={} partition={} seq={} id={}",
                    message.topic(), message.partition(), seq, message.id());
            return seq;
        }
    }

    public Message readMessageAt(String topic, int partition, long offset) {
        List<Message> list = loadMessages(topic, partition);
        if (offset < 0 || offset >= list.size()) {
            return null;
        }
        return list.get((int) offset);
    }

    public List<Message> readAllMessages(String topic, int partition) {
        return List.copyOf(loadMessages(topic, partition));
    }

    public long messageCount(String topic, int partition) {
        return loadMessages(topic, partition).size();
    }

    public long nextSeq(String topic, int partition) {
        return loadMessages(topic, partition).size();
    }

    private List<Message> loadMessages(String topic, int partition) {
        String cacheKey = topic + "#" + partition;
        return messageCache.computeIfAbsent(cacheKey, k -> {
            Path path = messageLog(topic, partition);
            if (!Files.exists(path)) {
                return Collections.synchronizedList(new ArrayList<>());
            }
            List<Message> list = Collections.synchronizedList(new ArrayList<>());
            for (String line : readLines(path)) {
                list.add(readValue(line, Message.class));
            }
            return list;
        });
    }

    // ==================== idempotency ====================

    /**
     * 保存幂等键。首次出现返回 true；已存在返回 false（不覆盖）。
     */
    public boolean saveIdempotencyKey(String topic, String key, String messageId, int partition, long offset) {
        String cacheKey = "topic#" + topic;
        synchronized (lockOf("idem#" + topic)) {
            Map<String, IdempotentRecord> map = loadIdempotency(topic);
            if (map.containsKey(key)) {
                return false;
            }
            appendLine(idemFile(topic), Map.of(
                    "key", key, "messageId", messageId, "partition", partition, "offset", offset));
            map.put(key, new IdempotentRecord(messageId, partition, offset));
            return true;
        }
    }

    public Optional<IdempotentRecord> findIdempotentMessage(String topic, String key) {
        return Optional.ofNullable(loadIdempotency(topic).get(key));
    }

    private Map<String, IdempotentRecord> loadIdempotency(String topic) {
        return idemCache.computeIfAbsent(topic, t -> {
            Map<String, IdempotentRecord> map = new LinkedHashMap<>();
            Path path = idemFile(t);
            if (Files.exists(path)) {
                for (String line : readLines(path)) {
                    Map<String, Object> row = readValue(line, new TypeReference<Map<String, Object>>() {
                    });
                    String key = (String) row.get("key");
                    String messageId = (String) row.get("messageId");
                    int partition = ((Number) row.get("partition")).intValue();
                    long offset = ((Number) row.get("offset")).longValue();
                    map.put(key, new IdempotentRecord(messageId, partition, offset));
                }
            }
            return Collections.synchronizedMap(map);
        });
    }

    // ==================== group offsets ====================

    public long loadCommittedOffset(String group, String topic, int partition) {
        String key = group + "|" + topic + "|" + partition;
        Long cached = offsetCache.get(key);
        if (cached != null) {
            return cached;
        }
        synchronized (lockOf("offsets#" + group)) {
            if (!offsetCache.containsKey(key)) {
                long value = -1L;
                Path path = offsetsFile(group);
                if (Files.exists(path)) {
                    for (String line : readLines(path)) {
                        Map<String, Object> row = readValue(line, new TypeReference<Map<String, Object>>() {
                        });
                        if (topic.equals(row.get("topic"))
                                && partition == ((Number) row.get("partition")).intValue()) {
                            value = ((Number) row.get("offset")).longValue();
                        }
                    }
                }
                offsetCache.put(key, value);
            }
            return offsetCache.get(key);
        }
    }

    /** 持久化位点（调用方负责单调性校验）。 */
    public void commitOffset(String group, String topic, int partition, long offset) {
        String key = group + "|" + topic + "|" + partition;
        synchronized (lockOf("offsets#" + group)) {
            appendLine(offsetsFile(group), Map.of(
                    "topic", topic, "partition", partition, "offset", offset, "committedAt", System.currentTimeMillis()));
            offsetCache.put(key, offset);
        }
    }

    // ==================== leases ====================

    public List<InFlightLease> loadLeases(String group) {
        return new ArrayList<>(leaseMap(group).values());
    }

    public InFlightLease findLease(String group, String deliveryId) {
        return leaseMap(group).get(deliveryId);
    }

    public void saveLease(InFlightLease lease) {
        synchronized (lockOf("leases#" + lease.group())) {
            appendLine(leasesFile(lease.group()), lease);
            leaseMap(lease.group()).put(lease.deliveryId(), lease);
        }
    }

    public void removeLease(String group, String deliveryId) {
        synchronized (lockOf("leases#" + group)) {
            Map<String, Object> tombstone = new LinkedHashMap<>();
            tombstone.put("deleted", deliveryId);
            appendLine(leasesFile(group), tombstone);
            leaseMap(group).remove(deliveryId);
        }
    }

    public List<String> listGroups() {
        Path dir = dataDir.resolve("groups");
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (var stream = Files.list(dir)) {
            return stream.filter(Files::isDirectory).map(p -> p.getFileName().toString())
                    .sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Map<String, InFlightLease> leaseMap(String group) {
        return leaseCache.computeIfAbsent(group, g -> {
            Map<String, InFlightLease> map = new LinkedHashMap<>();
            Path path = leasesFile(g);
            if (Files.exists(path)) {
                for (String line : readLines(path)) {
                    Map<String, Object> row = readValue(line, new TypeReference<Map<String, Object>>() {
                    });
                    if (row.containsKey("deleted")) {
                        map.remove((String) row.get("deleted"));
                    } else {
                        InFlightLease lease = mapper.convertValue(row, InFlightLease.class);
                        map.put(lease.deliveryId(), lease);
                    }
                }
            }
            return Collections.synchronizedMap(map);
        });
    }

    // ==================== dead letters ====================

    public void appendDeadLetter(DeadLetterEntry entry) {
        synchronized (lockOf("dlq#" + entry.group())) {
            List<DlqRecord> list = dlqMap(entry.group());
            appendLine(dlqFile(entry.group()), entry);
            list.add(new DlqRecord(entry, false));
        }
    }

    public List<DeadLetterEntry> loadDeadLetters(String group, String topic) {
        return loadAllDeadLetters(group).stream()
                .filter(e -> topic.equals(e.message().topic()))
                .toList();
    }

    public List<DeadLetterEntry> loadAllDeadLetters(String group) {
        return dlqMap(group).stream().filter(r -> !r.replayed()).map(DlqRecord::entry).toList();
    }

    /** 标记死信已重放；返回 false 表示条目不存在或已重放。 */
    public boolean markDeadLetterReplayed(String group, String deadLetterId) {
        synchronized (lockOf("dlq#" + group)) {
            List<DlqRecord> list = dlqMap(group);
            for (int i = 0; i < list.size(); i++) {
                DlqRecord r = list.get(i);
                if (r.entry().deadLetterId().equals(deadLetterId)) {
                    if (r.replayed()) {
                        return false;
                    }
                    appendLine(dlqFile(group), Map.of("replayed", deadLetterId));
                    list.set(i, new DlqRecord(r.entry(), true));
                    return true;
                }
            }
            return false;
        }
    }

    public DeadLetterEntry findDeadLetter(String group, String deadLetterId) {
        return dlqMap(group).stream()
                .filter(r -> r.entry().deadLetterId().equals(deadLetterId))
                .findFirst()
                .map(DlqRecord::entry)
                .orElse(null);
    }

    private List<DlqRecord> dlqMap(String group) {
        return dlqCache.computeIfAbsent(group, g -> {
            List<DlqRecord> list = Collections.synchronizedList(new ArrayList<>());
            Path path = dlqFile(g);
            if (Files.exists(path)) {
                java.util.Set<String> replayed = new java.util.HashSet<>();
                for (String line : readLines(path)) {
                    Map<String, Object> row = readValue(line, new TypeReference<Map<String, Object>>() {
                    });
                    if (row.containsKey("replayed")) {
                        replayed.add((String) row.get("replayed"));
                    } else {
                        DeadLetterEntry entry = mapper.convertValue(row, DeadLetterEntry.class);
                        list.add(new DlqRecord(entry, replayed.contains(entry.deadLetterId())));
                    }
                }
            }
            return list;
        });
    }

    // ==================== lifecycle ====================

    public void close() {
        // 当前实现写入即时刷盘，无延迟句柄需要关闭。
        log.info("PersistentStore closed, dataDir={}", dataDir);
    }

    // ==================== path / io helpers ====================

    private Path topicFile(String topic) {
        return dataDir.resolve("topics").resolve(topic + ".json");
    }

    private Path messageLog(String topic, int partition) {
        return dataDir.resolve("messages").resolve(topic).resolve("p" + partition + ".log");
    }

    private Path idemFile(String topic) {
        return dataDir.resolve("idem").resolve(topic + ".log");
    }

    private Path groupDir(String group) {
        Path dir = dataDir.resolve("groups").resolve(group);
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return dir;
    }

    private Path offsetsFile(String group) {
        return groupDir(group).resolve("offsets.log");
    }

    private Path leasesFile(String group) {
        return groupDir(group).resolve("leases.log");
    }

    private Path dlqFile(String group) {
        return groupDir(group).resolve("dlq.log");
    }

    private void appendLine(Path path, Object value) {
        try {
            Files.createDirectories(path.getParent());
            byte[] bytes = mapper.writeValueAsBytes(value);
            byte[] record = new byte[bytes.length + 1];
            System.arraycopy(bytes, 0, record, 0, bytes.length);
            record[bytes.length] = '\n';
            try (FileChannel ch = FileChannel.open(path,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                ch.write(java.nio.ByteBuffer.wrap(record));
                ch.force(true);
            }
        } catch (IOException e) {
            throw new MqException("写入持久化日志失败: " + path, e);
        }
    }

    private List<String> readLines(Path path) {
        try {
            return Files.readAllLines(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new MqException("读取持久化日志失败: " + path, e);
        }
    }

    private <T> T readValue(String line, Class<T> type) {
        try {
            return mapper.readValue(line, type);
        } catch (IOException e) {
            throw new MqException("反序列化失败: " + type.getSimpleName(), e);
        }
    }

    private <T> T readValue(String line, TypeReference<T> type) {
        try {
            return mapper.readValue(line, type);
        } catch (IOException e) {
            throw new MqException("反序列化失败: " + type.getType(), e);
        }
    }

    private void writeJson(Path path, Object value) {
        try {
            Files.createDirectories(path.getParent());
            byte[] bytes = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(value);
            try (FileChannel ch = FileChannel.open(path,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                ch.write(java.nio.ByteBuffer.wrap(bytes));
                ch.force(true);
            }
        } catch (IOException e) {
            throw new MqException("写入配置失败: " + path, e);
        }
    }

    private <T> T readJson(Path path, Class<T> type) {
        try {
            return mapper.readValue(Files.readAllBytes(path), type);
        } catch (IOException e) {
            throw new MqException("读取配置失败: " + path, e);
        }
    }

    // 供文档/调试展示当前生效的保留期等配置（保留扩展位）。
    static Duration requirePositive(Duration d) {
        if (d == null || d.isZero() || d.isNegative()) {
            throw new IllegalArgumentException("duration must be positive");
        }
        return d;
    }
}
