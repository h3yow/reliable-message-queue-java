package com.github.h3yow.reliablemessagequeuejava.queue.model;

import java.util.List;

/**
 * 死信批量重放结果。整批要么全部成功，要么整体被拒绝（抛异常，不产生任何结果）。
 *
 * <p>注意：筛选范围内只要含一条此前已重放的死信，整批也会被拒绝（不会静默跳过、只重放其余）；
 * 只有筛选范围内本来就一条死信都没有时，才返回 {@code selected=0}（无操作）。
 *
 * @param topic    主题
 * @param selected 命中筛选条件且参与本次重放的死信条数
 * @param items    每条死信对应的重放结果，顺序为分区/原位点升序
 */
public record BatchReplayResult(String topic, int selected, List<ReplayResult> items) {
}
