package com.github.h3yow.reliablemessagequeuejava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** HTTP 接口冒烟与拒绝原因可区分。 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class QueueControllerTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private String url(String path) {
        return "http://localhost:" + port + "/api/queue" + path;
    }

    @SuppressWarnings("unchecked")
    @Test
    void publishPollAckOverHttp() {
        String topic = "http-orders-" + java.util.UUID.randomUUID().toString().substring(0, 8);
        // 创建主题
        var created = rest.postForEntity(url("/topics/" + topic),
                new HttpEntity<>(Map.of("partitions", 1, "capacity", 100, "maxPayloadBytes", 10)),
                Map.class);
        assertEquals(HttpStatus.OK, created.getStatusCode());

        // 非法主题 -> 400 InvalidTopicException
        ResponseEntity<String> missing = rest.postForEntity(
                url("/topics/no-such-topic/messages"),
                new HttpEntity<>(Map.of("partitionKey", "k", "payload", "x")), String.class);
        assertEquals(HttpStatus.BAD_REQUEST, missing.getStatusCode());
        assertTrue(missing.getBody().contains("InvalidTopicException"));

        // 超长负载 -> 400 PayloadTooLargeException（可区分）
        ResponseEntity<String> tooLarge = rest.postForEntity(
                url("/topics/" + topic + "/messages"),
                new HttpEntity<>(Map.of("partitionKey", "k", "payload", "01234567890")),
                String.class);
        assertEquals(HttpStatus.BAD_REQUEST, tooLarge.getStatusCode());
        assertTrue(tooLarge.getBody().contains("PayloadTooLargeException"));

        // 正常发布两条
        var r1 = postMessage(topic, "k1", "idem-1", "hello");
        assertEquals(HttpStatus.OK, r1.getStatusCode());
        var r2 = postMessage(topic, "k1", "idem-2", "world");
        assertEquals(HttpStatus.OK, r2.getStatusCode());

        // 重复幂等键 -> 409 DuplicateMessageException
        var dup = postMessage(topic, "k2", "idem-1", "again");
        assertEquals(HttpStatus.CONFLICT, dup.getStatusCode());
        assertTrue(dup.getBody().contains("DuplicateMessageException"));

        // 容量耗尽：容量 100，这里仅验证状态码可区分（再灌到 100）
        // （为保持冒烟轻量，改验已有拒绝原因集合即可，容量耗尽由单测核心覆盖）

        // poll -> ack 流程
        Map<String, Object> delivery = rest.postForEntity(
                url("/groups/g1/topics/" + topic + "/poll"),
                HttpEntity.EMPTY, Map.class).getBody();
        assertNotNull(delivery);
        assertEquals("hello", ((Map<?, ?>) delivery.get("message")).get("payloadUtf8"));
        String deliveryId = (String) delivery.get("deliveryId");

        ResponseEntity<String> acked = rest.postForEntity(
                url("/groups/g1/acks/" + deliveryId), HttpEntity.EMPTY, String.class);
        assertEquals(HttpStatus.OK, acked.getStatusCode());

        // 重复确认 -> 400 DuplicateAckException
        ResponseEntity<String> dupAck = rest.postForEntity(
                url("/groups/g1/acks/" + deliveryId), HttpEntity.EMPTY, String.class);
        assertEquals(HttpStatus.BAD_REQUEST, dupAck.getStatusCode());
        assertTrue(dupAck.getBody().contains("DuplicateAckException"));

        // 第二条 nack 三次（maxAttempts=3）后进入死信
        Map<String, Object> d2 = rest.postForEntity(
                url("/groups/g1/topics/" + topic + "/poll"),
                HttpEntity.EMPTY, Map.class).getBody();
        String id2 = (String) d2.get("deliveryId");
        nack(id2, "boom-1");
        sleep(60);
        id2 = pollDeliveryId(topic);
        nack(id2, "boom-2");
        sleep(120);
        id2 = pollDeliveryId(topic);
        ResponseEntity<String> lastNack = rest.postForEntity(
                url("/groups/g1/nacks/" + id2 + "?error=boom-3"), HttpEntity.EMPTY, String.class);
        assertEquals(HttpStatus.OK, lastNack.getStatusCode());
        assertTrue(lastNack.getBody().contains("dead-lettered"));

        // 查询死信
        @SuppressWarnings("rawtypes")
        List dlq = rest.getForObject(url("/groups/g1/topics/" + topic + "/dlq"), List.class);
        assertEquals(1, dlq.size());
        String dlqId = (String) ((Map<?, ?>) dlq.get(0)).get("deadLetterId");

        // 死信重放 -> 可再次 poll
        var replay = rest.postForEntity(
                url("/groups/g1/dlq/" + dlqId + "/replay"), HttpEntity.EMPTY, Map.class);
        assertEquals(HttpStatus.OK, replay.getStatusCode());
        Map<String, Object> redelivered = rest.postForEntity(
                url("/groups/g1/topics/" + topic + "/poll"),
                HttpEntity.EMPTY, Map.class).getBody();
        assertEquals("world", ((Map<?, ?>) redelivered.get("message")).get("payloadUtf8"));
        rest.postForEntity(url("/groups/g1/acks/" + redelivered.get("deliveryId")),
                HttpEntity.EMPTY, String.class);
    }

    private ResponseEntity<String> postMessage(String topic, String key, String idem, String payload) {
        return rest.postForEntity(url("/topics/" + topic + "/messages"),
                new HttpEntity<>(Map.of("partitionKey", key, "idempotencyKey", idem, "payload", payload)),
                String.class);
    }

    @SuppressWarnings("unchecked")
    private String pollDeliveryId(String topic) {
        Map<String, Object> body = rest.postForEntity(
                url("/groups/g1/topics/" + topic + "/poll"), HttpEntity.EMPTY, Map.class).getBody();
        return (String) body.get("deliveryId");
    }

    private void nack(String deliveryId, String error) {
        var resp = rest.postForEntity(
                url("/groups/g1/nacks/" + deliveryId + "?error=" + error),
                HttpEntity.EMPTY, String.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
