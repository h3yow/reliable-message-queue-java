package com.github.h3yow.reliablemessagequeuejava.queue.web;

import com.github.h3yow.reliablemessagequeuejava.queue.ReliableMessageQueue;
import com.github.h3yow.reliablemessagequeuejava.queue.model.BatchReplayResult;
import com.github.h3yow.reliablemessagequeuejava.queue.model.DeadLetter;
import com.github.h3yow.reliablemessagequeuejava.queue.model.DeadLetterFilter;
import com.github.h3yow.reliablemessagequeuejava.queue.model.Delivery;
import com.github.h3yow.reliablemessagequeuejava.queue.model.PublishResult;
import com.github.h3yow.reliablemessagequeuejava.queue.model.ReplayResult;
import com.github.h3yow.reliablemessagequeuejava.queue.model.TopicConfig;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 可靠消息队列的本地 REST 接口。
 */
@RestController
@RequestMapping("/api/queue")
public class QueueController {

    private final ReliableMessageQueue queue;

    public QueueController(ReliableMessageQueue queue) {
        this.queue = queue;
    }

    @PostMapping("/topics")
    public Map<String, Object> createTopic(@RequestBody Map<String, Object> body) {
        TopicConfig config = new TopicConfig(
                String.valueOf(body.get("name")),
                intValue(body.get("partitionCount"), 1),
                intValue(body.get("capacity"), 10_000),
                intValue(body.get("maxPayloadBytes"), 1 << 20));
        queue.createTopic(config);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("topic", config.name());
        resp.put("partitionCount", config.partitionCount());
        resp.put("capacity", config.capacity());
        resp.put("maxPayloadBytes", config.maxPayloadBytes());
        return resp;
    }

    @PostMapping("/topics/{topic}/messages")
    public PublishResult publish(@PathVariable String topic, @RequestBody Map<String, Object> body) {
        String key = String.valueOf(body.getOrDefault("partitionKey", "default"));
        String idem = body.get("idempotencyKey") == null ? null : String.valueOf(body.get("idempotencyKey"));
        String text = body.get("payload") == null ? "" : String.valueOf(body.get("payload"));
        return queue.publishText(topic, key, idem, text);
    }

    @GetMapping("/topics/{topic}/poll")
    public List<Map<String, Object>> poll(@PathVariable String topic,
                                          @RequestParam String group,
                                          @RequestParam(defaultValue = "10") int max) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Delivery d : queue.poll(group, topic, max)) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("topic", d.topic());
            m.put("group", d.group());
            m.put("partition", d.partition());
            m.put("offset", d.offset());
            m.put("seq", d.seq());
            m.put("partitionKey", d.partitionKey());
            m.put("messageId", d.messageId());
            m.put("attempt", d.attempt());
            m.put("leaseToken", d.leaseToken());
            m.put("payload", new String(d.payload(), StandardCharsets.UTF_8));
            result.add(m);
        }
        return result;
    }

    @PostMapping("/topics/{topic}/ack")
    public Map<String, Object> ack(@PathVariable String topic,
                                   @RequestParam String group,
                                   @RequestBody Map<String, Object> body) {
        Delivery d = toDelivery(topic, group, body);
        queue.ack(d);
        return Map.of("status", "acked");
    }

    @PostMapping("/topics/{topic}/nack")
    public Map<String, Object> nack(@PathVariable String topic,
                                    @RequestParam String group,
                                    @RequestBody Map<String, Object> body) {
        Delivery d = toDelivery(topic, group, body);
        String error = body.get("error") == null ? "nack" : String.valueOf(body.get("error"));
        queue.nack(d, error);
        return Map.of("status", "nacked");
    }

    @PostMapping("/topics/{topic}/commit")
    public Map<String, Object> commit(@PathVariable String topic,
                                      @RequestParam String group,
                                      @RequestBody Map<String, Long> offsets) {
        Map<Integer, Long> partitionOffsets = new LinkedHashMap<>();
        offsets.forEach((k, v) -> partitionOffsets.put(Integer.parseInt(k), v));
        queue.commitSync(group, topic, partitionOffsets);
        return Map.of("status", "committed");
    }

    @GetMapping("/topics/{topic}/offset")
    public Map<String, Object> offset(@PathVariable String topic,
                                      @RequestParam String group,
                                      @RequestParam int partition) {
        return Map.of("committedOffset", queue.committedOffset(group, topic, partition));
    }

    @GetMapping("/topics/{topic}/deadletters")
    public List<DeadLetter> deadLetters(@PathVariable String topic,
                                        @RequestParam(required = false) String group) {
        return queue.listDeadLetters(group, topic);
    }

    @PostMapping("/deadletters/{deadLetterId}/replay")
    public ReplayResult replay(@PathVariable String deadLetterId,
                               @RequestParam(required = false) String group) {
        return queue.replayDeadLetter(deadLetterId, group);
    }

    @PostMapping("/topics/{topic}/deadletters/replay-batch")
    public BatchReplayResult replayBatch(@PathVariable String topic,
                                         @RequestParam(required = false) String group,
                                         @RequestParam(required = false) Integer partition,
                                         @RequestParam(required = false) Long deadAtFrom,
                                         @RequestParam(required = false) Long deadAtTo,
                                         @RequestBody(required = false) Map<String, Object> body) {
        // 查询参数优先，缺省时回退到 JSON body，方便 curl 与程序化调用。
        String g = group != null ? group : bodyString(body, "group");
        Integer p = partition != null ? partition : bodyInt(body, "partition");
        Long from = deadAtFrom != null ? deadAtFrom : bodyLong(body, "deadAtFrom");
        Long to = deadAtTo != null ? deadAtTo : bodyLong(body, "deadAtTo");
        return queue.replayDeadLetters(new DeadLetterFilter(topic, g, p, from, to));
    }

    @GetMapping("/topics/{topic}/partitions/{partition}/blocker")
    public Object partitionBlocker(@PathVariable String topic,
                                   @PathVariable int partition,
                                   @RequestParam(required = false) String group) {
        // 只给主题和分区（不传 group）时，返回该分区所有消费组各自的卡点聚合视图。
        if (group == null || group.isBlank()) {
            return queue.getPartitionBlockers(null, topic, partition);
        }
        return queue.getPartitionBlocker(group, topic, partition);
    }

    private static Delivery toDelivery(String topic, String group, Map<String, Object> body) {
        return new Delivery(topic, group,
                intValue(body.get("partition"), 0),
                longValue(body.get("offset"), 0L),
                longValue(body.get("seq"), 0L),
                String.valueOf(body.getOrDefault("partitionKey", "")),
                String.valueOf(body.get("messageId")),
                new byte[0],
                intValue(body.get("attempt"), 1),
                String.valueOf(body.get("leaseToken")));
    }

    private static int intValue(Object o, int def) {
        return o == null ? def : Integer.parseInt(String.valueOf(o));
    }

    private static long longValue(Object o, long def) {
        return o == null ? def : Long.parseLong(String.valueOf(o));
    }

    private static String bodyString(Map<String, Object> body, String key) {
        if (body == null || body.get(key) == null) {
            return null;
        }
        return String.valueOf(body.get(key));
    }

    private static Integer bodyInt(Map<String, Object> body, String key) {
        if (body == null || body.get(key) == null) {
            return null;
        }
        return Integer.valueOf(String.valueOf(body.get(key)));
    }

    private static Long bodyLong(Map<String, Object> body, String key) {
        if (body == null || body.get(key) == null) {
            return null;
        }
        return Long.valueOf(String.valueOf(body.get(key)));
    }
}
