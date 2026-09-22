package com.github.h3yow.reliablemessagequeuejava.queue;

import com.github.h3yow.reliablemessagequeuejava.queue.exception.DuplicateIdempotencyKeyException;
import com.github.h3yow.reliablemessagequeuejava.queue.model.DeadLetter;
import com.github.h3yow.reliablemessagequeuejava.queue.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.queue.model.PublishResult;
import com.github.h3yow.reliablemessagequeuejava.queue.model.ReplayResult;
import com.github.h3yow.reliablemessagequeuejava.queue.model.TopicConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeadLetterReplayTest {

    @Test
    void replayAppendsNewMessageWithoutBreakingOffsetsOrIdempotency(@TempDir Path dir) throws Exception {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 10_000, 10, 50, 2);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        PublishResult first = q.publishText("t", "k", null, "poison");
        q.publishText("t", "k", null, "after");

        // 首批包含毒消息与后续消息；后续消息直接确认，毒消息进入重试
        List<Delivery> firstBatch = q.poll("g", "t", 10);
        for (Delivery d : firstBatch) {
            if (d.messageId().equals(first.messageId())) {
                QueueTestSupport.logDelivery("nack", d);
                q.nack(d, "e1");
            } else {
                q.ack(d);
            }
        }
        Thread.sleep(30);
        Delivery d2 = pollOne(q, first.messageId());
        assertEquals(2, d2.attempt());
        q.nack(d2, "e2");

        List<DeadLetter> dead = q.listDeadLetters("g", "t");
        assertEquals(1, dead.size());
        String dlqId = dead.get(0).deadLetterId();
        long committedBeforeReplay = q.committedOffset("g", "t", 0);
        assertEquals(1L, committedBeforeReplay, "毒消息进死信且 after 已确认，连续位点应推进到 1");

        ReplayResult replay = q.replayDeadLetter(dlqId, "g");
        // 重放产生全新消息与新位点
        assertEquals(2L, replay.publishResult().offset());
        assertEquals(dead.get(0).originalOffset(), replay.deadLetter().originalOffset());
        assertTrue(replay.deadLetter().replayed());

        // 旧位点没有被破坏
        assertEquals(committedBeforeReplay, q.committedOffset("g", "t", 0));

        // 重放消息是唯一未消费消息
        List<Delivery> remaining = q.poll("g", "t", 10);
        assertEquals(1, remaining.size());
        assertEquals(replay.publishResult().offset(), remaining.get(0).offset());
        assertEquals("poison", QueueTestSupport.text(remaining.get(0)));
        QueueTestSupport.logDelivery("consume-replay", remaining.get(0));
        q.ack(remaining.get(0));
        long finalOffset = q.committedOffset("g", "t", 0);
        assertEquals(replay.publishResult().offset(), finalOffset,
                "确认重放消息后位点应推进到新消息位点");

        // 重放幂等：不能重复重放
        assertThrows(DuplicateIdempotencyKeyException.class,
                () -> q.replayDeadLetter(dlqId, "g"));
    }

    private static Delivery pollOne(ReliableMessageQueue q, String messageId) {
        List<Delivery> batch = q.poll("g", "t", 10);
        return batch.stream().filter(d -> d.messageId().equals(messageId)).findFirst().orElseThrow();
    }
}
