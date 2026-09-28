package com.github.h3yow.reliablemessagequeuejava.queue.exception;

/**
 * 批量重放被整体拒绝：筛选范围内任意一条死信当前无法重新进入投递流程，
 * 整批原样拒绝，不改动任何已有数据（消息数量、死信状态、已确认进度、容量占用、重试状态都不变）。
 *
 * <p>两类典型原因与重试方式：
 * <ul>
 *   <li>容量不足（可恢复）：腾出容量后用<em>完全相同</em>的筛选条件再次提交即可整体成功；</li>
 *   <li>筛选范围内含已重放记录（不可原样重试）：本系统不做“跳过已重放、只处理其余”的部分生效，
 *       需缩小筛选范围（如补充 group/partition/死信时间范围）后重新提交。</li>
 * </ul>
 */
public class BatchReplayRejectedException extends QueueException {

    public BatchReplayRejectedException(String message) {
        super("BATCH_REPLAY_REJECTED", message);
    }
}
