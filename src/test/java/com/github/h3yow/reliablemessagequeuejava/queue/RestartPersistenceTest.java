package com.github.h3yow.reliablemessagequeuejava.queue;

import com.github.h3yow.reliablemessagequeuejava.queue.model.DeadLetter;
import com.github.h3yow.reliablemessagequeuejava.queue.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.queue.model.TopicConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RestartPersistenceTest {

    @Test
    void persistedStateSurvivesRestart(@TempDir Path dir) throws Exception {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 10_000, 10, 50, 2);
        q.createTopic(new TopicConfig("t", 1, 100, 1 << 20));
        q.publishText("t", "k", null, "ok");
        q.publishText("t", "k", null, "poison");
        q.publishText("t", "k", null, "pending");

        List<Delivery> batch = q.poll("g", "t", 10);
        q.ack(batch.get(0)); // ok
        q.nack(batch.get(1), "fail1");
        Thread.sleep(30);
        List<Delivery> again = q.poll("g", "t", 10);
        Delivery poison = again.stream().filter(d -> QueueTestSupport.text(d).equals("poison"))
                .findFirst().orElseThrow();
        q.nack(poison, "fail2"); // 达上限进死信
        q.close();

        // 重启（调用 start 模拟应用重启，旧未确认租约立即失效）
        ReliableMessageQueue reopened = QueueTestSupport.reopen(dir, 10_000, 10, 50, 2);
        reopened.start();
        assertEquals(1L, reopened.committedOffset("g", "t", 0),
                "已确认位点重启后保持；offset0 确认、offset1 poison 进死信，连续位点推进到 1");

        List<DeadLetter> dead = reopened.listDeadLetters("g", "t");
        assertEquals(1, dead.size());
        assertEquals("poison", QueueTestSupport.textPayload(dead.get(0).payload()));
        assertEquals(2, dead.get(0).attempts());

        // pending(offset2) 仍可消费；poison(offset1) 不再投递
        List<Delivery> rest = reopened.poll("g", "t", 10);
        assertEquals(1, rest.size());
        assertEquals("pending", QueueTestSupport.text(rest.get(0)));
        reopened.ack(rest.get(0));
        assertEquals(2L, reopened.committedOffset("g", "t", 0));
        reopened.close();
    }
}
