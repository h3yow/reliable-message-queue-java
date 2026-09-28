package com.github.h3yow.reliablemessagequeuejava.queue.exception;

/**
 * 批量重放被整体拒绝：批次筛选范围内任意一条当前无法重新进入投递流程——例如主题容量不足、
 * 该死信此前已被重放（含被单条重放）、重放幂等键冲突或负载超长——整批原样拒绝，
 * 不跳过任何条目、不改动任何已有数据（消息数量、死信状态、已确认进度、容量占用、重试状态均不变）。
 * 容量等“临时性”问题处理完后，可用完全相同的筛选条件重新提交并整体成功；
 * 因包含已重放记录被拒时，请缩小筛选条件后重新提交。
 */
public class BatchReplayRejectedException extends QueueException {

    public BatchReplayRejectedException(String message) {
        super("BATCH_REPLAY_REJECTED", message);
    }
}
