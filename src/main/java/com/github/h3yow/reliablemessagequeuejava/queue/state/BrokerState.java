package com.github.h3yow.reliablemessagequeuejava.queue.state;

import com.github.h3yow.reliablemessagequeuejava.queue.model.DeadLetter;
import com.github.h3yow.reliablemessagequeuejava.queue.model.StoredMessage;
import com.github.h3yow.reliablemessagequeuejava.queue.model.TopicConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Broker 的完整可持久化状态快照。
 */
public class BrokerState {
    public Map<String, TopicConfig> topics = new HashMap<>();
    public List<StoredMessage> messages = new ArrayList<>();
    public List<DeadLetter> deadLetters = new ArrayList<>();
    public Map<String, Set<String>> idempotencyKeys = new HashMap<>();
    public Map<String, GroupState> groups = new HashMap<>();
    /** topic -> 长度分区数 的数组：下标 p 为该分区下一条位点。 */
    public Map<String, long[]> partitionMeta = new HashMap<>();
    /** topic -> 主题级单调递增消息序号（下一条）。 */
    public Map<String, Long> topicSequences = new HashMap<>();
}
