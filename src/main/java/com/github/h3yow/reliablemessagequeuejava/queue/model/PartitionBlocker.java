package com.github.h3yow.reliablemessagequeuejava.queue.model;

/**
 * 单个消费组视角下“挡住分区”的那一条消息。
 *
 * @param group           消费组
 * @param messageId       挡路消息 id
 * @param partition       分区
 * @param offset          位点
 * @param seq             主题级序号
 * @param partitionKey    分区键
 * @param attempts        已经重试的次数（nack 累计次数，未 nack 过为 0）
 * @param state           卡点状态：DELIVERABLE / LEASED / LEASE_EXPIRED / BACKOFF
 * @param nextDeliverAt   下一次可被投递的时间（epoch millis）
 * @param leaseExpiresAt  租约到期时间（无租约时为 null）
 * @param lastError       最近一次消费失败错误（无则为 null）
 */
public record PartitionBlocker(String group,
                               String messageId,
                               int partition,
                               long offset,
                               long seq,
                               String partitionKey,
                               int attempts,
                               String state,
                               long nextDeliverAt,
                               Long leaseExpiresAt,
                               String lastError) {

    /** 已可被下一次 poll 立即投递（含租约已超时未确认、应用重启后尚未重新投递的情况）。 */
    public static final String DELIVERABLE = "DELIVERABLE";
    /** 已投递给消费者，租约尚未到期。 */
    public static final String LEASED = "LEASED";
    /** 曾投递但租约已超时，等待下一次 poll 重新投递。 */
    public static final String LEASE_EXPIRED = "LEASE_EXPIRED";
    /** 消费失败，正处于指数退避等待中。 */
    public static final String BACKOFF = "BACKOFF";
}
