package com.github.h3yow.reliablemessagequeuejava.tests;

import org.junit.jupiter.api.Test;

/** 失败重试、退避上限、死信与重放。 */
class RetryAndDlqTest {
    @Test
    void failedMessageRetriesWithBackoffAndEntersDlq() { /* TODO */ }

    @Test
    void otherPartitionsAreNotBlockedByFailingOne() { /* TODO */ }

    @Test
    void deadLetterReplayDoesNotBreakOffsetsOrIdempotency() { /* TODO */ }
}
