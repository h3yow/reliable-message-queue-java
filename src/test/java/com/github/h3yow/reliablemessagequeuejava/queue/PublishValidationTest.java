package com.github.h3yow.reliablemessagequeuejava.queue;

import com.github.h3yow.reliablemessagequeuejava.queue.exception.DuplicateIdempotencyKeyException;
import com.github.h3yow.reliablemessagequeuejava.queue.exception.PayloadTooLargeException;
import com.github.h3yow.reliablemessagequeuejava.queue.exception.QueueException;
import com.github.h3yow.reliablemessagequeuejava.queue.exception.TopicCapacityExceededException;
import com.github.h3yow.reliablemessagequeuejava.queue.exception.TopicNotFoundException;
import com.github.h3yow.reliablemessagequeuejava.queue.model.TopicConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PublishValidationTest {

    @Test
    void rejectsIllegalTopicOversizePayloadFullCapacityAndDuplicateKey(@TempDir Path dir) {
        ReliableMessageQueue q = QueueTestSupport.newQueue(dir, 5_000, 10, 100, 3);
        q.createTopic(new TopicConfig("orders", 2, 3, 10));

        // 非法主题：不存在
        QueueException notFound = assertThrows(TopicNotFoundException.class,
                () -> q.publishText("missing", "k", null, "x"));
        assertEquals("TOPIC_NOT_FOUND", notFound.getCode());

        // 超长负载
        byte[] big = "0123456789A".getBytes(); // 11 > 10
        QueueException tooLarge = assertThrows(PayloadTooLargeException.class,
                () -> q.publish("orders", "k", null, big));
        assertEquals("PAYLOAD_TOO_LARGE", tooLarge.getCode());

        // 容量耗尽
        q.publishText("orders", "k1", null, "a");
        q.publishText("orders", "k2", null, "b");
        q.publishText("orders", "k3", null, "c");
        QueueException full = assertThrows(TopicCapacityExceededException.class,
                () -> q.publishText("orders", "k4", null, "d"));
        assertEquals("TOPIC_CAPACITY_EXCEEDED", full.getCode());

        // 重复幂等键不产生第二次可见投递
        ReliableMessageQueue q2 = QueueTestSupport.newQueue(dir.resolve("sub2"),
                5_000, 10, 100, 3);
        q2.createTopic(new TopicConfig("idem", 1, 100, 1000));
        q2.publishText("idem", "k", "same-key", "first");
        QueueException dup = assertThrows(DuplicateIdempotencyKeyException.class,
                () -> q2.publishText("idem", "k2", "same-key", "second"));
        assertEquals("DUPLICATE_IDEMPOTENCY_KEY", dup.getCode());
        // 只有一条可见消息
        var deliveries = q2.poll("g", "idem", 10);
        assertEquals(1, deliveries.size());
        assertEquals("first", QueueTestSupport.text(deliveries.get(0)));
    }
}
