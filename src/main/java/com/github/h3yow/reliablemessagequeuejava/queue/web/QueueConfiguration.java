package com.github.h3yow.reliablemessagequeuejava.queue.web;

import com.github.h3yow.reliablemessagequeuejava.queue.ReliableMessageQueue;
import com.github.h3yow.reliablemessagequeuejava.queue.model.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 队列 Spring 装配。
 */
@Configuration
@EnableConfigurationProperties(QueueProperties.class)
public class QueueConfiguration {

    private static final Logger log = LoggerFactory.getLogger(QueueConfiguration.class);

    @Bean(name = "reliableMessageQueue", initMethod = "start", destroyMethod = "close")
    public ReliableMessageQueue reliableMessageQueue(QueueProperties properties) {
        Path dataFile = Path.of(properties.getDataFile());
        if (dataFile.getParent() != null) {
            try {
                Files.createDirectories(dataFile.getParent());
            } catch (Exception e) {
                throw new IllegalStateException("无法创建数据目录: " + dataFile.getParent(), e);
            }
        }
        return new ReliableMessageQueue(dataFile,
                properties.getLeaseMillis(),
                properties.getBackoffBaseMillis(),
                properties.getBackoffMaxMillis(),
                properties.getMaxAttempts());
    }

    /** 本地演示：确保存在 demo 主题并发一条欢迎消息，方便直接 curl 验证。 */
    @Bean
    public ApplicationRunner queueDemoInitializer(ReliableMessageQueue queue) {
        return args -> {
            try {
                queue.createTopic(new TopicConfig("demo-topic", 4, 10_000, 1 << 20));
            } catch (RuntimeException alreadyExists) {
                // 主题已存在则忽略
            }
            try {
                queue.publishText("demo-topic", "demo-key", "bootstrap-welcome",
                        "hello reliable queue @ " + java.time.Instant.now());
                log.info("已发布演示消息到 demo-topic");
            } catch (RuntimeException duplicate) {
                log.info("演示消息此前已发布，跳过");
            }
        };
    }
}
