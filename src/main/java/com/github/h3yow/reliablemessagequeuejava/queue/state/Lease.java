package com.github.h3yow.reliablemessagequeuejava.queue.state;

/**
 * 一条消息在某个消费组上的在途租约（已投递未确认）。
 */
public class Lease {
    public String token;
    public String group;
    public String topic;
    public int partition;
    public long offset;
    public String messageId;
    public int attempt;
    public long leasedAt;
    public long expiresAt;
}
