package com.github.h3yow.reliablemessagequeuejava.queue.web;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 队列本地配置。
 */
@ConfigurationProperties(prefix = "rmq")
public class QueueProperties {
    private String dataFile = "data/queue-state.json";
    private long leaseMillis = 30_000L;
    private long backoffBaseMillis = 1_000L;
    private long backoffMaxMillis = 30_000L;
    private int maxAttempts = 5;
    private long reaperPeriodMillis = 1_000L;

    public String getDataFile() { return dataFile; }
    public void setDataFile(String dataFile) { this.dataFile = dataFile; }
    public long getLeaseMillis() { return leaseMillis; }
    public void setLeaseMillis(long leaseMillis) { this.leaseMillis = leaseMillis; }
    public long getBackoffBaseMillis() { return backoffBaseMillis; }
    public void setBackoffBaseMillis(long backoffBaseMillis) { this.backoffBaseMillis = backoffBaseMillis; }
    public long getBackoffMaxMillis() { return backoffMaxMillis; }
    public void setBackoffMaxMillis(long backoffMaxMillis) { this.backoffMaxMillis = backoffMaxMillis; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public long getReaperPeriodMillis() { return reaperPeriodMillis; }
    public void setReaperPeriodMillis(long reaperPeriodMillis) { this.reaperPeriodMillis = reaperPeriodMillis; }
}
