package com.github.h3yow.reliablemessagequeuejava.queue.model;

import java.util.List;

/**
 * 批量重放结果。
 *
 * @param requested 本次筛选命中并请求重放的死信条数
 * @param replayed  实际重放成功的条数（整批原子：要么等于 requested，要么为 0）
 * @param results   每条死信的重放结果，顺序与死信进入死信前在分区中的先后顺序一致
 */
public record BatchReplayResult(int requested, int replayed, List<ReplayResult> results) {
}
