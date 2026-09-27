package com.github.h3yow.reliablemessagequeuejava.queue.exception;

/**
 * 批量重放被整批拒绝：批次中有任何一条当前无法重放（容量不足、死信已被重放等），
 * 整批原样拒绝，不做任何改动。修复问题后可用相同条件原样重试。
 */
public class BatchReplayRejectedException extends QueueException {
    public BatchReplayRejectedException(String message) {
        super("BATCH_REPLAY_REJECTED", message);
    }
}
