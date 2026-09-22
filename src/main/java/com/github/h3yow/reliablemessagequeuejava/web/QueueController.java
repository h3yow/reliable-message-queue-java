package com.github.h3yow.reliablemessagequeuejava.web;

import com.github.h3yow.reliablemessagequeuejava.core.MessageQueue;
import com.github.h3yow.reliablemessagequeuejava.model.DeadLetterEntry;
import com.github.h3yow.reliablemessagequeuejava.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.model.PublishReceipt;
import com.github.h3yow.reliablemessagequeuejava.model.TopicConfig;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 本地 HTTP 接口，便于手动验证发布/消费/确认/重试/死信重放。
 *
 * <pre>
 * POST /api/queue/topics/{name}                      创建主题
 * POST /api/queue/topics/{name}/messages             发布消息
 * POST /api/queue/groups/{group}/topics/{topic}/poll 拉取
 * POST /api/queue/groups/{group}/acks/{deliveryId}   确认
 * POST /api/queue/groups/{group}/nacks/{deliveryId}  否认（重试/死信）
 * GET  /api/queue/groups/{group}/topics/{topic}/dlq  查询死信
 * POST /api/queue/groups/{group}/dlq/{id}/replay     死信重放
 * </pre>
 */
@RestController
@RequestMapping("/api/queue")
public class QueueController {

    private final MessageQueue messageQueue;

    public QueueController(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    @PostMapping("/topics/{name}")
    public Map<String, Object> createTopic(@PathVariable String name,
                                           @RequestBody(required = false) Map<String, Object> body) {
        int partitions = intOr(body, "partitions", 4);
        long capacity = longOr(body, "capacity", 100_000L);
        int maxPayloadBytes = intOr(body, "maxPayloadBytes", 64 * 1024);
        messageQueue.createTopic(new TopicConfig(name, partitions, capacity, maxPayloadBytes, Duration.ZERO));
        return Map.of("topic", name, "partitions", partitions,
                "capacity", capacity, "maxPayloadBytes", maxPayloadBytes);
    }

    @PostMapping("/topics/{name}/messages")
    public Map<String, Object> publish(@PathVariable String name,
                                       @RequestBody Map<String, Object> body) {
        String key = str(body, "partitionKey", null);
        String idem = str(body, "idempotencyKey", null);
        byte[] payload = decodePayload(body);
        PublishReceipt receipt = messageQueue.publish(name, key, idem, payload);
        return Map.of("messageId", receipt.messageId(), "topic", receipt.topic(),
                "partition", receipt.partition(), "offset", receipt.offset());
    }

    @PostMapping("/groups/{group}/topics/{topic}/poll")
    public Map<String, Object> poll(@PathVariable String group, @PathVariable String topic) {
        Optional<Delivery> delivery = messageQueue.poll(group, topic);
        return delivery.<Map<String, Object>>map(d -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("deliveryId", d.deliveryId());
            m.put("group", d.group());
            m.put("partition", d.partition());
            m.put("offset", d.offset());
            m.put("attempt", d.attempt());
            m.put("leasedAt", d.leasedAt());
            m.put("leasedUntil", d.leasedUntil());
            m.put("message", Map.of(
                    "id", d.message().id(),
                    "partitionKey", d.message().partitionKey(),
                    "idempotencyKey", d.message().idempotencyKey() == null
                            ? "" : d.message().idempotencyKey(),
                    "seq", d.message().seq(),
                    "payloadBase64",
                    Base64.getEncoder().encodeToString(d.message().payload()),
                    "payloadUtf8", new String(d.message().payload(), StandardCharsets.UTF_8)));
            return m;
        }).orElseGet(() -> Map.of("empty", true));
    }

    @PostMapping("/groups/{group}/acks/{deliveryId}")
    public Map<String, Object> ack(@PathVariable String group, @PathVariable String deliveryId) {
        messageQueue.ack(group, deliveryId);
        return Map.of("status", "acked", "deliveryId", deliveryId);
    }

    @PostMapping("/groups/{group}/nacks/{deliveryId}")
    public Map<String, Object> nack(@PathVariable String group, @PathVariable String deliveryId,
                                    @RequestParam(required = false) String error,
                                    @RequestBody(required = false) Map<String, Object> body) {
        String reason = error != null ? error : (body == null ? null : str(body, "error", null));
        int next = messageQueue.nack(group, deliveryId, reason);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("deliveryId", deliveryId);
        if (next < 0) {
            result.put("status", "dead-lettered");
        } else {
            result.put("status", "scheduled-retry");
            result.put("attempt", next);
        }
        return result;
    }

    @GetMapping("/groups/{group}/topics/{topic}/dlq")
    public List<Map<String, Object>> deadLetters(@PathVariable String group,
                                                 @PathVariable String topic) {
        return messageQueue.deadLetters(group, topic).stream()
                .map(QueueController::toDlqJson).toList();
    }

    @GetMapping("/groups/{group}/topics/{topic}/partitions/{partition}/offset")
    public Map<String, Object> getOffset(@PathVariable String group, @PathVariable String topic,
                                         @PathVariable int partition) {
        return Map.of("group", group, "topic", topic, "partition", partition,
                "committedOffset", messageQueue.committedOffset(group, topic, partition));
    }

    @PostMapping("/groups/{group}/topics/{topic}/partitions/{partition}/offset")
    public Map<String, Object> commitOffset(@PathVariable String group, @PathVariable String topic,
                                            @PathVariable int partition,
                                            @RequestBody Map<String, Object> body) {
        long offset = ((Number) body.get("offset")).longValue();
        messageQueue.commitOffset(group, topic, partition, offset);
        return Map.of("group", group, "topic", topic, "partition", partition,
                "committedOffset", offset, "status", "committed");
    }

    @PostMapping("/groups/{group}/dlq/{deadLetterId}/replay")
    public Map<String, Object> replay(@PathVariable String group,
                                      @PathVariable String deadLetterId) {
        PublishReceipt receipt = messageQueue.replayDeadLetter(group, deadLetterId);
        return Map.of("deadLetterId", deadLetterId, "status", "replayed",
                "messageId", receipt.messageId(), "topic", receipt.topic(),
                "partition", receipt.partition(), "offset", receipt.offset());
    }

    private static Map<String, Object> toDlqJson(DeadLetterEntry e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("deadLetterId", e.deadLetterId());
        m.put("group", e.group());
        m.put("topic", e.message().topic());
        m.put("partitionKey", e.message().partitionKey());
        m.put("partition", e.partition());
        m.put("offset", e.offset());
        m.put("attempts", e.attempts());
        m.put("lastError", e.lastError());
        m.put("deadAt", e.deadAt());
        m.put("payloadUtf8", new String(e.message().payload(), StandardCharsets.UTF_8));
        return m;
    }

    private static byte[] decodePayload(Map<String, Object> body) {
        if (body.containsKey("payloadBase64")) {
            return Base64.getDecoder().decode((String) body.get("payloadBase64"));
        }
        if (body.containsKey("payload")) {
            Object p = body.get("payload");
            return String.valueOf(p).getBytes(StandardCharsets.UTF_8);
        }
        return new byte[0];
    }

    private static int intOr(Map<String, Object> body, String key, int def) {
        if (body == null || body.get(key) == null) {
            return def;
        }
        return ((Number) body.get(key)).intValue();
    }

    private static long longOr(Map<String, Object> body, String key, long def) {
        if (body == null || body.get(key) == null) {
            return def;
        }
        return ((Number) body.get(key)).longValue();
    }

    private static String str(Map<String, Object> body, String key, String def) {
        if (body == null || body.get(key) == null) {
            return def;
        }
        return String.valueOf(body.get(key));
    }
}
