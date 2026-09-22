package com.github.h3yow.reliablemessagequeuejava.config;

import com.github.h3yow.reliablemessagequeuejava.core.MessageQueue;
import com.github.h3yow.reliablemessagequeuejava.core.RedeliveryService;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(QueueProperties.class)
public class QueueAutoConfiguration {

    @Bean(destroyMethod = "close")
    public MessageQueue messageQueue(QueueProperties properties) {
        return new MessageQueue(properties);
    }

    @Bean
    public RedeliveryService redeliveryService(MessageQueue messageQueue) {
        return new RedeliveryService(messageQueue);
    }
}
