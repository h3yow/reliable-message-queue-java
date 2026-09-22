package com.github.h3yow.reliablemessagequeuejava.tests;

import org.junit.jupiter.api.Test;

/** 发布/消费基础语义：分区顺序、消费组独立、幂等、拒绝原因可区分。 */
class PublishConsumeTest {
    @Test
    void partitionOrderingWithinKey() { /* TODO */ }

    @Test
    void independentGroupsConsumeIndependently() { /* TODO */ }

    @Test
    void duplicateIdempotencyKeyRejected() { /* TODO */ }

    @Test
    void invalidTopicOversizePayloadAndCapacityRejectedDifferently() { /* TODO */ }
}
