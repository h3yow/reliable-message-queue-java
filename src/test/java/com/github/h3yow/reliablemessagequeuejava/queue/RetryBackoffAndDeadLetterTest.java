package com.github.h3yow.reliablemessagequeuejava.queue;

import com.github.h3yow.reliablemessagequeuejava.queue.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.queue.model.PublishResult;
import com.github.h3yow.reliablemessagequeuejava.queue.model.TopicConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryBackoffAndDeadLetterTest {

    @Test
    void retriesWithBackoffThenDeadLettersWithoutBlockingOthers(@TempDir Path dir) throws Exception {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 10_000, 200, 500, 3);
        q.createTopic(new TopicConfig("t", 4, 100, 1 << 20));

        PublishResult bad = q.publishText("t", "bad-key", null, "poison");
        PublishResult goodPublished;
        int guard = 0;
        PublishResult candidate = q.publishText("t", "good-key", null, "fine");
        while (candidate.partition() == bad.partition() && guard++ < 10) {
            candidate = q.publishText("t", "good-key-" + guard, null, "fine-" + guard);
        }
        goodPublished = candidate;
        assertTrue(goodPublished.partition() != bad.partition(), "应存在不同分区的正常消息");

        // 第一次投递：毒消息 nack，好消息 ack 不受影响
        List<Delivery> batch = q.poll("g", "t", 10);
        Delivery poison = batch.stream()
                .filter(d -> d.messageId().equals(bad.messageId())).findFirst().orElseThrow();
        Delivery healthy = batch.stream()
                .filter(d -> d.messageId().equals(goodPublished.messageId())).findFirst().orElseThrow();
        QueueTestSupport.logDelivery("nack", poison);
        q.nack(poison, "boom-1");
        q.ack(healthy);

        // 退避期内毒消息不可见，但其它分区消费正常（好消息已 ack，再发一条别的分区消息验证不阻塞）
        assertTrue(q.poll("g", "t", 10).stream().noneMatch(d -> d.messageId().equals(bad.messageId())));

        // 第 2 次尝试
        Thread.sleep(250);
        Delivery retry2 = pollOne(q, bad.messageId());
        assertEquals(2, retry2.attempt());
        q.nack(retry2, "boom-2");

        Thread.sleep(450);
        Delivery retry3 = pollOne(q, bad.messageId());
        assertEquals(3, retry3.attempt());
        QueueTestSupport.logDelivery("retry", retry3);
        q.nack(retry3, "boom-3");

        // 达到 maxAttempts=3 -> 死信
        var dead = q.listDeadLetters("g", "t");
        assertEquals(1, dead.size());
        assertEquals(bad.messageId(), dead.get(0).originalMessageId());
        assertEquals(3, dead.get(0).attempts());
        assertEquals("poison", QueueTestSupport.textPayload(dead.get(0).payload()));
        // 死信消息不再被投递，且位点不因它卡死（good-key 所在分区位点已推进）
        long goodCommitted = q.committedOffset("g", "t", goodPublished.partition());
        assertEquals(goodPublished.offset(), goodCommitted);
        // 毒消息分区：毒消息进死信视为已解决，位点推进到它
        long badCommitted = q.committedOffset("g", "t", bad.partition());
        assertEquals(bad.offset(), badCommitted);
    }

    private static Delivery pollOne(ReliableMessageQueue q, String messageId) {
        List<Delivery> batch = q.poll("g", "t", 10);
        return batch.stream().filter(d -> d.messageId().equals(messageId)).findFirst().orElseThrow();
    }
}
