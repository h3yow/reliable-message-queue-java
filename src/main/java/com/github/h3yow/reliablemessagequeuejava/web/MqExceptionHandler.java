package com.github.h3yow.reliablemessagequeuejava.web;

import com.github.h3yow.reliablemessagequeuejava.exception.MqException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/** 拒绝原因可区分：异常类型 -> HTTP 状态码 + reason。 */
@RestControllerAdvice
public class MqExceptionHandler {

    @ExceptionHandler(MqException.class)
    public ResponseEntity<Map<String, String>> handle(MqException ex) {
        return ResponseEntity.badRequest().body(Map.of("reason", ex.getClass().getSimpleName(), "message", ex.getMessage()));
    }
}
