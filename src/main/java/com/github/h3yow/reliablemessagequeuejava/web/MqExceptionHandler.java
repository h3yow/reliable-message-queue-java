package com.github.h3yow.reliablemessagequeuejava.web;

import com.github.h3yow.reliablemessagequeuejava.exception.CrossGroupCommitException;
import com.github.h3yow.reliablemessagequeuejava.exception.DuplicateAckException;
import com.github.h3yow.reliablemessagequeuejava.exception.DuplicateMessageException;
import com.github.h3yow.reliablemessagequeuejava.exception.InvalidTopicException;
import com.github.h3yow.reliablemessagequeuejava.exception.LeaseExpiredException;
import com.github.h3yow.reliablemessagequeuejava.exception.MqException;
import com.github.h3yow.reliablemessagequeuejava.exception.OffsetRegressionException;
import com.github.h3yow.reliablemessagequeuejava.exception.PayloadTooLargeException;
import com.github.h3yow.reliablemessagequeuejava.exception.TopicCapacityExceededException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 拒绝原因可区分：具体异常类型 -> HTTP 状态码 + 机器可读 reason。
 */
@RestControllerAdvice
public class MqExceptionHandler {

    @ExceptionHandler({InvalidTopicException.class, PayloadTooLargeException.class,
            CrossGroupCommitException.class, OffsetRegressionException.class,
            DuplicateAckException.class, LeaseExpiredException.class})
    public ResponseEntity<Map<String, Object>> badRequest(MqException ex) {
        return build(HttpStatus.BAD_REQUEST, ex);
    }

    @ExceptionHandler(DuplicateMessageException.class)
    public ResponseEntity<Map<String, Object>> conflict(DuplicateMessageException ex) {
        return build(HttpStatus.CONFLICT, ex);
    }

    @ExceptionHandler(TopicCapacityExceededException.class)
    public ResponseEntity<Map<String, Object>> insufficientStorage(TopicCapacityExceededException ex) {
        return build(HttpStatus.INSUFFICIENT_STORAGE, ex);
    }

    @ExceptionHandler(MqException.class)
    public ResponseEntity<Map<String, Object>> internal(MqException ex) {
        return build(HttpStatus.INTERNAL_SERVER_ERROR, ex);
    }

    private static ResponseEntity<Map<String, Object>> build(HttpStatus status, MqException ex) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", status.value());
        body.put("reason", ex.getClass().getSimpleName());
        body.put("message", ex.getMessage());
        return ResponseEntity.status(status).body(body);
    }
}
