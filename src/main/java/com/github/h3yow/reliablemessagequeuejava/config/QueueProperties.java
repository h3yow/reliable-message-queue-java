package com.github.h3yow.reliablemessagequeuejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 队列本地配置（application.properties 以 rmq.* 覆盖）。
 */
@ConfigurationProperties(prefix = "rmq")
public class QueueProperties {

    /** 本地持久化根目录 */
    private String dataDir = "data/rmq";
    /** 单次发布最大负载字节数（主题级配置优先） */
    private int defaultMaxPayloadBytes = 64 * 1024;
    /** 主题容量（消息条数），主题级配置优先 */
    private long defaultCapacity = 100_000L;
    /** 默认分区数 */
    private int defaultPartitions = 4;
    /** 消费租约时长，超时未确认即重新投递 */
    private Duration visibilityTimeout = Duration.ofSeconds(30);
    /** 最大消费尝试次数，超过后进入死信 */
    private int maxAttempts = 5;
    /** 初始退避 */
    private Duration backoffBase = Duration.ofMillis(200);
    /** 退避上限 */
    private Duration backoffMax = Duration.ofSeconds(30);

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }
    public int getDefaultMaxPayloadBytes() { return defaultMaxPayloadBytes; }
    public void setDefaultMaxPayloadBytes(int v) { this.defaultMaxPayloadBytes = v; }
    public long getDefaultCapacity() { return defaultCapacity; }
    public void setDefaultCapacity(long v) { this.defaultCapacity = v; }
    public int getDefaultPartitions() { return defaultPartitions; }
    public void setDefaultPartitions(int v) { this.defaultPartitions = v; }
    public Duration getVisibilityTimeout() { return visibilityTimeout; }
    public void setVisibilityTimeout(Duration v) { this.visibilityTimeout = v; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int v) { this.maxAttempts = v; }
    public Duration getBackoffBase() { return backoffBase; }
    public void setBackoffBase(Duration v) { this.backoffBase = v; }
    public Duration getBackoffMax() { return backoffMax; }
    public void setBackoffMax(Duration v) { this.backoffMax = v; }
}
