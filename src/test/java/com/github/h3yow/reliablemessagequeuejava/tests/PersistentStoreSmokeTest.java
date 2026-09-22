package com.github.h3yow.reliablemessagequeuejava.tests;

import com.github.h3yow.reliablemessagequeuejava.model.DeadLetterEntry;
import com.github.h3yow.reliablemessagequeuejava.model.InFlightLease;
import com.github.h3yow.reliablemessagequeuejava.model.Message;
import com.github.h3yow.reliablemessagequeuejava.model.TopicConfig;
import com.github.h3yow.reliablemessagequeuejava.storage.PersistentStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PersistentStoreSmokeTest {

    @TempDir
    Path tmp;

    private Message msg(int seq) {
        return new Message("id-" + seq, "orders", "k1", null,
                ("payload-" + seq).getBytes(), 0, seq, 1000L + seq);
    }

    @Test
    void appendMessagesSurviveReopen() {
        new PersistentStore(tmp.toString()).close();
        PersistentStore store = new PersistentStore(tmp.toString());
        assertEquals(0L, store.appendMessage(msg(0)));
        assertEquals(1L, store.appendMessage(msg(1)));
        store.close();

        PersistentStore reopened = new PersistentStore(tmp.toString());
        assertEquals(2, reopened.readAllMessages("orders", 0).size());
        assertEquals("id-1", reopened.readMessageAt("orders", 0, 1).id());
        assertEquals(2L, reopened.nextSeq("orders", 0));
    }

    @Test
    void offsetsLeasesDlqAndIdempotencySurviveReopen() {
        PersistentStore store = new PersistentStore(tmp.toString());
        store.saveTopicConfig(new TopicConfig("orders", 2, 10L, 1024, Duration.ofMinutes(1)));
        assertTrue(store.saveIdempotencyKey("orders", "key-1", "id-1", 0, 0L));
        assertFalse(store.saveIdempotencyKey("orders", "key-1", "id-1", 0, 0L));
        store.commitOffset("g1", "orders", 0, 5L);
        store.saveLease(new InFlightLease("d1", "g1", "orders", 1, 6L, 1, 1L, 2L, 3L, null));
        Message m = msg(6);
        store.appendDeadLetter(new DeadLetterEntry("dlq-1", "g1", m, 1, 6L, 5, "boom", 9L));
        store.close();

        PersistentStore reopened = new PersistentStore(tmp.toString());
        assertEquals("orders", reopened.loadTopicConfig("orders").orElseThrow().name());
        assertEquals("id-1", reopened.findIdempotentMessage("orders", "key-1").orElseThrow().messageId());
        assertEquals(5L, reopened.loadCommittedOffset("g1", "orders", 0));
        assertEquals(-1L, reopened.loadCommittedOffset("g1", "orders", 1));
        List<InFlightLease> leases = reopened.loadLeases("g1");
        assertEquals(1, leases.size());
        assertEquals("d1", leases.get(0).deliveryId());
        assertEquals(1, reopened.loadAllDeadLetters("g1").size());
        assertTrue(reopened.markDeadLetterReplayed("g1", "dlq-1"));
        assertEquals(0, reopened.loadAllDeadLetters("g1").size());
        assertFalse(reopened.markDeadLetterReplayed("g1", "dlq-1"));
    }
}
