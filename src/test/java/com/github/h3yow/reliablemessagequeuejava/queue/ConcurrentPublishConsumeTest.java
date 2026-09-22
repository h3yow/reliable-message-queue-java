package com.github.h3yow.reliablemessagequeuejava.queue;

import com.github.h3yow.reliablemessagequeuejava.queue.exception.DuplicateIdempotencyKeyException;
import com.github.h3yow.reliablemessagequeuejava.queue.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.queue.model.TopicConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConcurrentPublishConsumeTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentPublishConsumeTest.class);

    @Test
    void concurrentPublishAndConsumeIsLosslessAndIdempotent(@TempDir Path dir) throws Exception {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 30_000, 10, 100, 3);
        q.createTopic(new TopicConfig("events", 4, 1_000_000, 1 << 20));

        int producers = 4;
        int perProducer = 100;
        ExecutorService pool = Executors.newFixedThreadPool(producers + 2);
        CountDownLatch publishDone = new CountDownLatch(producers);
        AtomicInteger duplicateRejected = new AtomicInteger();

        // 并发发布：同一批幂等键被多个生产者重复提交，只有首次应成功
        for (int p = 0; p < producers; p++) {
            final int pid = p;
            pool.submit(() -> {
                try {
                    for (int i = 0; i < perProducer; i++) {
                        String key = "key-" + (i % 20); // 制造同分区热点
                        String idem = "idem-" + i;       // 跨生产者共享的幂等键
                        try {
                            q.publishText("events", key, idem, "p" + pid + "-" + i);
                        } catch (DuplicateIdempotencyKeyException dup) {
                            duplicateRejected.incrementAndGet();
                        }
                    }
                } finally {
                    publishDone.countDown();
                }
            });
        }

        // 两个消费组并发消费
        Set<String> consumedG1 = ConcurrentHashMap.newKeySet();
        Set<String> consumedG2 = ConcurrentHashMap.newKeySet();
        CountDownLatch consumersStarted = new CountDownLatch(2);
        for (String group : new String[]{"g1", "g2"}) {
            Set<String> sink = group.equals("g1") ? consumedG1 : consumedG2;
            pool.submit(() -> {
                consumersStarted.countDown();
                while (true) {
                    List<Delivery> batch = q.poll(group, "events", 32);
                    if (batch.isEmpty()) {
                        if (publishDone.getCount() == 0) {
                            // 再确认一轮，避免发布刚结束时的边界
                            batch = q.poll(group, "events", 32);
                            if (batch.isEmpty()) {
                                return;
                            }
                        } else {
                            try {
                                Thread.sleep(5);
                            } catch (InterruptedException e) {
                                return;
                            }
                            continue;
                        }
                    }
                    for (Delivery d : batch) {
                        boolean fresh = sink.add(d.messageId());
                        assertTrue(fresh, "重复可见投递: group=" + group + " offset=" + d.offset());
                        QueueTestSupport.logDelivery("consume-" + group, d);
                        q.ack(d);
                    }
                }
            });
        }

        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "并发任务应在时限内完成");

        int expectedUnique = perProducer; // idem-0..idem-99，每键只成功一次
        assertEquals(expectedUnique, consumedG1.size(), "g1 不应丢消息");
        assertEquals(expectedUnique, consumedG2.size(), "g2 不应丢消息");
        assertEquals(producers * perProducer - expectedUnique, duplicateRejected.get(),
                "重复幂等键必须被拒绝");

        // 位点单调：每个分区最终位点 = 该分区最大 offset
        for (int p = 0; p < 4; p++) {
            long off1 = q.committedOffset("g1", "events", p);
            long off2 = q.committedOffset("g2", "events", p);
            assertTrue(off1 >= -1 && off2 >= -1);
        }
        // 全部分区位点之和 + 分区数 = 总消息数（每个分区最终位点为 maxOffset）
        long sumOffsets = 0;
        for (int p = 0; p < 4; p++) {
            long o = q.committedOffset("g1", "events", p);
            sumOffsets += o + 1;
        }
        assertEquals(expectedUnique, sumOffsets, "所有分区位点推进后应覆盖全部消息");
        log.info("并发测试完成 唯一消息={} 拒绝重复={}", expectedUnique, duplicateRejected.get());
    }
}
