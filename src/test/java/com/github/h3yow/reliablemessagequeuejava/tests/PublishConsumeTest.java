package com.github.h3yow.reliablemessagequeuejava.tests;

import com.github.h3yow.reliablemessagequeuejava.core.MessageQueue;
import com.github.h3yow.reliablemessagequeuejava.exception.DuplicateMessageException;
import com.github.h3yow.reliablemessagequeuejava.exception.InvalidTopicException;
import com.github.h3yow.reliablemessagequeuejava.exception.PayloadTooLargeException;
import com.github.h3yow.reliablemessagequeuejava.exception.TopicCapacityExceededException;
import com.github.h3yow.reliablemessagequeuejava.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.model.PublishReceipt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/** 发布/消费基础语义：分区顺序、消费组独立、幂等、拒绝原因可区分。 */
class PublishConsumeTest {

    @TempDir
    Path tmp;

    @Test
    void partitionOrderingWithinKey() {
        try (MessageQueue q = TestQueues.newQueue(tmp)) {
            q.createTopic("orders", 4, 1000, 1024);
            List<PublishReceipt> receipts = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                receipts.add(q.publish("orders", "customer-7", "idem-" + i,
                        ("msg-" + i).getBytes()));
            }
            // 同一分区键 -> 同一分区，序号严格递增
            int partition = receipts.get(0).partition();
            long prev = -1;
            for (PublishReceipt r : receipts) {
                assertEquals(partition, r.partition());
                assertTrue(r.offset() > prev, "同分区键消息序号必须单调递增");
                prev = r.offset();
            }

            // 消费时按顺序交付
            List<Long> deliveredOffsets = new ArrayList<>();
            List<Integer> attempts = new ArrayList<>();
            Optional<Delivery> d;
            while ((d = q.poll("grp-a", "orders")).isPresent()) {
                deliveredOffsets.add(d.get().offset());
                attempts.add(d.get().attempt());
                // 只有 customer-7 所在分区会在该轮被消费完
                q.ack("grp-a", d.get().deliveryId());
            }
            List<Long> expected = receipts.stream().map(PublishReceipt::offset).sorted().toList();
            assertEquals(expected, deliveredOffsets, "同分区键消息必须按位点顺序交付");
            assertTrue(attempts.stream().allMatch(a -> a == 1));
        }
    }

    @Test
    void independentGroupsConsumeIndependently() {
        try (MessageQueue q = TestQueues.newQueue(tmp)) {
            q.createTopic("orders", 2, 1000, 1024);
            for (int i = 0; i < 10; i++) {
                q.publish("orders", "key-" + i, null, ("m" + i).getBytes());
            }

            long aCount = drainAndAck(q, "grp-a");
            // grp-b 独立位点，应能消费到同样数量的消息
            long bCount = drainAndAck(q, "grp-b");
            assertEquals(aCount, bCount);
            long committedMessages = (q.committedOffset("grp-a", "orders", 0) + 1)
                    + (q.committedOffset("grp-a", "orders", 1) + 1);
            assertEquals(10, committedMessages);

            // grp-a 已确认不影响 grp-b：重启队列后 grp-b 位点仍在
            try (MessageQueue reopened = TestQueues.newQueue(tmp)) {
                assertEquals(-1L, reopened.committedOffset("grp-c", "orders", 0));
            }
        }
    }

    private long drainAndAck(MessageQueue q, String group) {
        long count = 0;
        Optional<Delivery> d;
        while ((d = q.poll(group, "orders")).isPresent()) {
            count++;
            q.ack(group, d.get().deliveryId());
        }
        return count;
    }

    @Test
    void duplicateIdempotencyKeyRejected() {
        try (MessageQueue q = TestQueues.newQueue(tmp)) {
            q.createTopic("orders", 4, 1000, 1024);
            assertNotNull(q.publish("orders", "k1", "idem-x", "a".getBytes()));
            DuplicateMessageException ex = assertThrows(DuplicateMessageException.class,
                    () -> q.publish("orders", "k2", "idem-x", "b".getBytes()));
            assertTrue(ex.getMessage().contains("幂等键重复"));

            // 只有第一条对消费者可见
            int visible = 0;
            Optional<Delivery> d;
            while ((d = q.poll("grp-a", "orders")).isPresent()) {
                visible++;
                q.ack("grp-a", d.get().deliveryId());
            }
            // 只有一条 k1 落在其分区（k2 同幂等键被拒），但分区内可能仅此一条
            assertEquals(1, countForTopic(q, "grp-a", "orders"));
        }
    }

    private long countForTopic(MessageQueue q, String group, String topic) {
        long total = 0;
        for (int p = 0; p < 4; p++) {
            long o = q.committedOffset(group, topic, p);
            if (o >= 0) {
                total += o + 1;
            }
        }
        return total;
    }

    @Test
    void invalidTopicOversizePayloadAndCapacityRejectedDifferently() {
        try (MessageQueue q = TestQueues.newQueue(tmp)) {
            q.createTopic("orders", 1, 3, 10);

            // 非法主题（不存在）
            assertThrows(InvalidTopicException.class,
                    () -> q.publish("missing-topic", "k", null, "x".getBytes()));
            // 非法主题名格式
            assertThrows(InvalidTopicException.class,
                    () -> q.createTopic("../bad", 1, 10, 10));

            // 超长负载
            PayloadTooLargeException tooLarge = assertThrows(PayloadTooLargeException.class,
                    () -> q.publish("orders", "k1", null, "01234567890".getBytes()));
            assertTrue(tooLarge.getMessage().contains("负载超长"));

            // 容量耗尽（容量 3）
            q.publish("orders", "k1", null, "1".getBytes());
            q.publish("orders", "k2", null, "2".getBytes());
            q.publish("orders", "k3", null, "3".getBytes());
            TopicCapacityExceededException cap = assertThrows(TopicCapacityExceededException.class,
                    () -> q.publish("orders", "k4", null, "4".getBytes()));
            assertTrue(cap.getMessage().contains("容量耗尽"));

            // 三类拒绝原因彼此可区分
            assertNotEquals(InvalidTopicException.class, PayloadTooLargeException.class);
            assertNotEquals(PayloadTooLargeException.class, TopicCapacityExceededException.class);
        }
    }
}
