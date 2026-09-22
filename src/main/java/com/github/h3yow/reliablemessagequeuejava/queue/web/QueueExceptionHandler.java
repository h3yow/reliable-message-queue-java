package com.github.h3yow.reliablemessagequeuejava.queue.web;

import com.github.h3yow.reliablemessagequeuejava.queue.exception.QueueException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * 把队列异常映射为带可区分错误码的 HTTP 响应。
 */
@RestControllerAdvice
public class QueueExceptionHandler {

    @ExceptionHandler(QueueException.class)
    public ResponseEntity<Map<String, Object>> handle(QueueException e) {
        HttpStatus status = switch (e.getCode()) {
            case "TOPIC_NOT_FOUND", "DEAD_LETTER_NOT_FOUND" -> HttpStatus.NOT_FOUND;
            case "DUPLICATE_IDEMPOTENCY_KEY", "TOPIC_CAPACITY_EXCEEDED",
                 "OFFSET_ROLLBACK", "CROSS_GROUP_COMMIT", "DUPLICATE_ACK" -> HttpStatus.CONFLICT;
            default -> HttpStatus.BAD_REQUEST;
        };
        return ResponseEntity.status(status).body(Map.of(
                "code", e.getCode(),
                "message", e.getMessage() == null ? "" : e.getMessage()));
    }
}
