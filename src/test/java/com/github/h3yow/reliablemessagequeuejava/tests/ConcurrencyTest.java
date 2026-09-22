package com.github.h3yow.reliablemessagequeuejava.tests;

import com.github.h3yow.reliablemessagequeuejava.config.QueueProperties;
import com.github.h3yow.reliablemessagequeuejava.core.MessageQueue;
import com.github.h3yow.reliablemessagequeuejava.exception.DuplicateMessageException;
import com.github.h3yow.reliablemessagequeuejava.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.model.PublishReceipt;
import com.github.h3yow.reliablemessagequeuejava.storage.PersistentStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** 并发发布与消费：不丢失、幂等不重复、位点单调。 */
class ConcurrencyTest {

    @TempDir
    Path tmp;

    @Test
    void concurrentPublishAndConsumeNoLossNoDuplicate() throws Exception {
        QueueProperties props = TestQueues.properties(tmp, Duration.ofSeconds(2),
                5, Duration.ofMillis(1), Duration.ofMillis(10));
        int producerCount = 8;
        int perProducer = 125;               // 共 1000 条唯一消息
        int duplicateWriters = 4;            // 4 个线程抢发相同 100 个幂等键
        int consumerCount = 6;
        int totalUnique = producerCount * perProducer;

        Set<String> acceptedIds = ConcurrentHashMap.newKeySet();
        AtomicInteger duplicateRejections = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(producerCount + duplicateWriters + consumerCount);
        CountDownLatch start = new CountDownLatch(1);

        try (MessageQueue q = new MessageQueue(props, new PersistentStore(tmp.toString()))) {
            q.createTopic("orders", 4, totalUnique * 2L, 4096);

            // ---- 唯一消息生产者 ----
            for (int p = 0; p < producerCount; p++) {
                int producerId = p;
                pool.submit(() -> {
                    await(start);
                    for (int i = 0; i < perProducer; i++) {
                        String key = "key-" + (i % 20);
                        String idem = "producer-" + producerId + "-msg-" + i;
                        PublishReceipt r = q.publish("orders", key, idem,
                                ("payload-" + producerId + "-" + i).getBytes());
                        acceptedIds.add(r.messageId());
                    }
                });
            }

            // ---- 幂等键竞争：100 个键，每个被 4 个线程同时发布，仅 100 条可见 ----
            for (int w = 0; w < duplicateWriters; w++) {
                int writerId = w;
                pool.submit(() -> {
                    await(start);
                    for (int i = 0; i < 100; i++) {
                        try {
                            PublishReceipt r = q.publish("orders", "dup-key-" + i,
                                    "shared-idem-" + i, ("dup-" + writerId + "-" + i).getBytes());
                            acceptedIds.add(r.messageId());
                        } catch (DuplicateMessageException e) {
                            duplicateRejections.incrementAndGet();
                        }
                    }
                });
            }

            // ---- 消费者：全部确认；记录交付的消息 id，验证不重复、分区内有序 ----
            Set<String> consumedMessageIds = ConcurrentHashMap.newKeySet();
            List<String> duplicateDelivery = Collections.synchronizedList(new ArrayList<>());
            // 每个 (partition) 记录上一个已交付的消息序号，验证同分区顺序
            ConcurrentHashMap<Integer, Long> lastSeenSeq = new ConcurrentHashMap<>();
            AtomicInteger consumed = new AtomicInteger();
            AtomicInteger acks = new AtomicInteger();

            for (int c = 0; c < consumerCount; c++) {
                pool.submit(() -> {
                    await(start);
                    int idleRounds = 0;
                    while (idleRounds < 50) {
                        Optional<Delivery> d = q.poll("grp-shared", "orders");
                        if (d.isEmpty()) {
                            idleRounds++;
                            sleep(10);
                            continue;
                        }
                        idleRounds = 0;
                        Delivery delivery = d.get();
                        long prev = lastSeenSeq.getOrDefault(delivery.partition(), -1L);
                        // 同分区交付顺序必须单调（窗口为 1，重投也只会给同一条）
                        assertTrue(delivery.offset() >= prev,
                                "同分区交付乱序: partition=" + delivery.partition());
                        lastSeenSeq.put(delivery.partition(), delivery.offset());
                        if (!consumedMessageIds.add(delivery.message().id())) {
                            duplicateDelivery.add(delivery.message().id());
                        }
                        consumed.incrementAndGet();
                        q.ack("grp-shared", delivery.deliveryId());
                        acks.incrementAndGet();
                    }
                });
            }

            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "并发任务必须在超时前结束");

            // ---- 断言 ----
            int expectedVisible = totalUnique + 100;       // 1000 唯一 + 100 去重消息
            assertEquals(expectedVisible, acceptedIds.size(),
                    "生产者：唯一消息 + 去重后消息数量必须精确");
            assertEquals(100 * (duplicateWriters - 1), duplicateRejections.get(),
                    "300 次重复幂等键发布必须全部被明确拒绝");

            assertEquals(expectedVisible, consumed.get(), "不丢失：可见消息必须全部被消费");
            assertEquals(expectedVisible, acks.get(), "每条消息恰好确认一次");
            assertEquals(0, duplicateDelivery.size(), "不重复：同一消息不得有两次可见投递");
            assertEquals(expectedVisible, consumedMessageIds.size(),
                    "消费侧消息 id 集合大小必须等于可见消息数");

            // 位点：每个分区 (offset+1) 之和等于可见消息总数
            long committedMessages = 0;
            for (int p = 0; p < 4; p++) {
                long offset = q.committedOffset("grp-shared", "orders", p);
                assertTrue(offset >= 0);
                committedMessages += offset + 1;
            }
            assertEquals(expectedVisible, committedMessages, "位点持久化的消息总数必须精确");
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
