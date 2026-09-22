package com.github.h3yow.reliablemessagequeuejava.queue;

import com.github.h3yow.reliablemessagequeuejava.queue.exception.CrossGroupCommitException;
import com.github.h3yow.reliablemessagequeuejava.queue.exception.DuplicateAckException;
import com.github.h3yow.reliablemessagequeuejava.queue.exception.OffsetRollbackException;
import com.github.h3yow.reliablemessagequeuejava.queue.exception.QueueException;
import com.github.h3yow.reliablemessagequeuejava.queue.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.queue.model.TopicConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OffsetCommitValidationTest {

    @Test
    void rejectsRollbackGapDuplicateAckAndCrossGroupWithDistinctCodes(@TempDir Path dir) {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 10_000, 10, 100, 3);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        for (int i = 0; i < 5; i++) {
            q.publishText("t", "k", null, "m" + i);
        }

        // g1 拿到前两条，确认第一条 -> 位点 0
        List<Delivery> g1Batch = q.poll("g1", "t", 5);
        Delivery first = g1Batch.get(0);
        q.ack(first);
        assertEquals(0L, q.committedOffset("g1", "t", 0));

        // 重复确认（ack）-> DUPLICATE_ACK
        QueueException dupAck = assertThrows(DuplicateAckException.class, () -> q.ack(first));
        assertEquals("DUPLICATE_ACK", dupAck.getCode());

        // 手动提交相同位点（重复提交）-> DUPLICATE_ACK，与真正回退区分
        QueueException same = assertThrows(DuplicateAckException.class,
                () -> q.commitSync("g1", "t", Map.of(0, 0L)));
        assertEquals("DUPLICATE_ACK", same.getCode());

        // 提交更小位点（真正回退）-> OFFSET_ROLLBACK
        QueueException back = assertThrows(OffsetRollbackException.class,
                () -> q.commitSync("g1", "t", Map.of(0, -1L)));
        assertEquals("OFFSET_ROLLBACK", back.getCode());

        // 跳过未确认的 offset1，直接提交到 2 -> OFFSET_ROLLBACK（位点缺口）
        QueueException gap = assertThrows(OffsetRollbackException.class,
                () -> q.commitSync("g1", "t", Map.of(0, 2L)));
        assertEquals("OFFSET_ROLLBACK", gap.getCode());

        // 跨消费组确认：g2 拿到一条属于 g2 的投递，尝试由 g1 去确认 -> CROSS_GROUP_COMMIT
        Delivery g2first = q.poll("g2", "t", 1).get(0);
        Delivery asG1 = new Delivery(g2first.topic(), "g1", g2first.partition(), g2first.offset(),
                g2first.seq(), g2first.partitionKey(), g2first.messageId(), g2first.payload(),
                g2first.attempt(), g2first.leaseToken());
        QueueException cross = assertThrows(CrossGroupCommitException.class, () -> q.ack(asG1));
        assertEquals("CROSS_GROUP_COMMIT", cross.getCode());
    }
}
