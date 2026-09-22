package com.github.h3yow.reliablemessagequeuejava.queue;

import com.github.h3yow.reliablemessagequeuejava.queue.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.queue.model.TopicConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class RedeliveryAndOffsetTest {

    @Test
    void redeliversAfterLeaseTimeoutAndRestart(@TempDir Path dir) throws Exception {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 300, 10, 100, 3);
        q.createTopic(new TopicConfig("t", 2, 100, 1 << 20));
        q.publishText("t", "k1", null, "hello");
        q.publishText("t", "k2", null, "world");

        List<Delivery> first = q.poll("g", "t", 10);
        assertEquals(2, first.size());
        long committedBefore = q.committedOffset("g", "t", first.get(0).partition());
        assertEquals(-1, committedBefore);

        // 租约超时后应能再次消费
        Thread.sleep(450);
        List<Delivery> second = q.poll("g", "t", 10);
        assertEquals(2, second.size());
        QueueTestSupport.logDelivery("redeliver-timeout", second.get(0));

        // 模拟应用重启：未 ack 的消息在新实例中仍可消费
        q.close();
        ReliableMessageQueue reopened = QueueTestSupport.reopen(dir, 300, 10, 100, 3);
        Thread.sleep(350); // 让旧租约到期
        List<Delivery> third = reopened.poll("g", "t", 10);
        assertEquals(2, third.size());
        QueueTestSupport.logDelivery("redeliver-restart", third.get(0));

        // 确认后位点推进，且消息不再投递
        for (Delivery d : third) {
            reopened.ack(d);
        }
        assertEquals(0, reopened.poll("g", "t", 10).size());
        for (Delivery d : third) {
            long c = reopened.committedOffset("g", "t", d.partition());
            assertEquals(d.offset(), c, "确认后位点应推进到该消息位点");
        }
        reopened.close();
    }
}
