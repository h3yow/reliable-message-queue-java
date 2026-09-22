package com.github.h3yow.reliablemessagequeuejava.queue;

import com.github.h3yow.reliablemessagequeuejava.queue.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.queue.model.TopicConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionOrderingAndGroupsTest {

    private static final Logger log = LoggerFactory.getLogger(PartitionOrderingAndGroupsTest.class);

    @Test
    void sameKeyStaysOrderedAndGroupsConsumeIndependently(@TempDir Path dir) {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 5_000, 10, 100, 3);
        q.createTopic(new TopicConfig("orders", 4, 10_000, 1 << 20));

        int n = 50;
        for (int i = 0; i < n; i++) {
            // 全部使用同一分区键 -> 同分区，必须严格有序
            q.publishText("orders", "hot-key", null, "msg-" + i);
        }

        List<Delivery> g1 = drainAndAck(q, "g1", "orders");
        List<Delivery> g2 = drainAndAck(q, "g2", "orders");

        assertEquals(n, g1.size());
        assertEquals(n, g2.size());
        for (int i = 0; i < n; i++) {
            assertEquals("msg-" + i, QueueTestSupport.text(g1.get(i)), "g1 顺序被破坏");
            assertEquals("msg-" + i, QueueTestSupport.text(g2.get(i)), "g2 顺序被破坏");
            assertEquals(g1.get(i).partition(), g2.get(i).partition());
        }
        // 位点单调推进到 n-1
        long last = q.committedOffset("g1", "orders", g1.get(0).partition());
        assertEquals(n - 1, last);
        log.info("顺序消费完成 topic=orders key=hot-key 数量={} 最终位点={}", n, last);
        assertTrue(g1.get(n - 1).seq() > n - 1 || g1.get(n - 1).seq() == n - 1);
    }

    private static List<Delivery> drainAndAck(ReliableMessageQueue q, String group, String topic) {
        List<Delivery> all = new ArrayList<>();
        while (true) {
            List<Delivery> batch = q.poll(group, topic, 16);
            if (batch.isEmpty()) {
                return all;
            }
            for (Delivery d : batch) {
                QueueTestSupport.logDelivery("consume-" + group, d);
                all.add(d);
                q.ack(d);
            }
        }
    }
}
