package com.github.h3yow.reliablemessagequeuejava.web;

import com.github.h3yow.reliablemessagequeuejava.core.MessageQueue;
import org.springframework.web.bind.annotation.*;

/**
 * 本地 HTTP 接口，便于手动验证发布/消费/确认/重试/死信重放。
 */
@RestController
@RequestMapping("/api/queue")
public class QueueController {

    private final MessageQueue messageQueue;

    public QueueController(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    @PostMapping("/topics/{name}")
    public Object createTopic(@PathVariable String name, @RequestBody(required = false) Object body) { return null; }

    @PostMapping("/topics/{name}/messages")
    public Object publish(@PathVariable String name, @RequestBody(required = false) Object body) { return null; }

    @PostMapping("/groups/{group}/topics/{topic}/poll")
    public Object poll(@PathVariable String group, @PathVariable String topic) { return null; }

    @PostMapping("/groups/{group}/acks/{deliveryId}")
    public Object ack(@PathVariable String group, @PathVariable String deliveryId) { return null; }

    @PostMapping("/groups/{group}/nacks/{deliveryId}")
    public Object nack(@PathVariable String group, @PathVariable String deliveryId,
                       @RequestParam(required = false) String error) { return null; }

    @GetMapping("/groups/{group}/topics/{topic}/dlq")
    public Object deadLetters(@PathVariable String group, @PathVariable String topic) { return null; }

    @PostMapping("/groups/{group}/dlq/{deadLetterId}/replay")
    public Object replay(@PathVariable String group, @PathVariable String deadLetterId) { return null; }
}
