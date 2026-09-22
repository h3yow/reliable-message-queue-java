package com.github.h3yow.reliablemessagequeuejava.tests;

import com.github.h3yow.reliablemessagequeuejava.config.QueueProperties;
import com.github.h3yow.reliablemessagequeuejava.core.MessageQueue;
import com.github.h3yow.reliablemessagequeuejava.storage.PersistentStore;

import java.nio.file.Path;
import java.time.Duration;

/** 测试辅助：在临时目录构造可调参数的队列。 */
final class TestQueues {

    private TestQueues() {
    }

    static QueueProperties properties(Path dataDir) {
        return properties(dataDir, Duration.ofSeconds(30), 5, Duration.ofMillis(50), Duration.ofSeconds(5));
    }

    static QueueProperties properties(Path dataDir, Duration visibilityTimeout,
                                      int maxAttempts, Duration backoffBase, Duration backoffMax) {
        QueueProperties p = new QueueProperties();
        p.setDataDir(dataDir.toString());
        p.setVisibilityTimeout(visibilityTimeout);
        p.setMaxAttempts(maxAttempts);
        p.setBackoffBase(backoffBase);
        p.setBackoffMax(backoffMax);
        return p;
    }

    static MessageQueue newQueue(Path dataDir) {
        return new MessageQueue(properties(dataDir));
    }

    static MessageQueue newQueue(QueueProperties p) {
        return new MessageQueue(p, new PersistentStore(p.getDataDir()));
    }
}
