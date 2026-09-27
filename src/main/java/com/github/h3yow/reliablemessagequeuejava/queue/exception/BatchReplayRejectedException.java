package com.github.h3yow.reliablemessagequeuejava.queue.exception;

/**
 * 批量重放被整体拒绝：批次中任意一条当前无法重新进入投递流程（如容量不足、已被重放、跨组），
 * 整批原样拒绝，不改动任何已有数据。处理完问题后可用完全相同的筛选条件重新提交。
 */
public class BatchReplayRejectedException extends QueueException {

    public BatchReplayRejectedException(String message) {
        super("BATCH_REPLAY_REJECTED", message);
    }
}
