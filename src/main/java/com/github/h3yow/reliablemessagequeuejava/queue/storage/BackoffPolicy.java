package com.github.h3yow.reliablemessagequeuejava.queue.storage;

/**
 * 有上限的指数退避策略。
 *
 * <p>第 {@code attempt} 次失败后，下次投递前的等待时间为：
 * {@code min(baseMillis * 2^(attempt-1), maxMillis)}。
 */
public final class BackoffPolicy {

    private BackoffPolicy() {
    }

    /**
     * @param attempt     已经失败的次数（从 1 开始）
     * @param baseMillis  基础退避
     * @param maxMillis   退避上限
     * @return 下次投递前需要等待的毫秒数
     */
    public static long nextDelayMillis(int attempt, long baseMillis, long maxMillis) {
        if (attempt < 1) {
            return 0L;
        }
        long delay = baseMillis;
        for (int i = 1; i < attempt && delay < maxMillis; i++) {
            long doubled = delay << 1;
            if (doubled <= 0 || doubled > maxMillis) {
                return maxMillis;
            }
            delay = doubled;
        }
        return Math.min(delay, maxMillis);
    }
}
