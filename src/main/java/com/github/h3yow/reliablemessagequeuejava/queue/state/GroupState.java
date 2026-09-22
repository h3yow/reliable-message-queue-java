package com.github.h3yow.reliablemessagequeuejava.queue.state;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 消费组状态：已提交位点、已解决位点与在途租约。
 */
public class GroupState {
    public String group;
    public String topic;
    public Map<Integer, Long> committedOffsets = new HashMap<>();
    public Set<String> resolvedKeys = new HashSet<>();
    public Map<String, Lease> leasesByKey = new HashMap<>();
    public Map<String, NackState> nackByKey = new HashMap<>();
}
