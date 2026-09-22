package com.github.h3yow.reliablemessagequeuejava.queue.state;

/**
 * 某条消息在某个消费组上的失败退避状态。
 */
public class NackState {
    public int attempts;
    public long nextVisibleAt;
    public String lastError;
}
