package com.example.demo.controller;


import com.example.demo.bo.MessageBO;
import com.example.demo.config.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    @Autowired
    private RedisTemplate<String, Object> streamRedisTemplate;

    @GetMapping("/pushMessage")
    public String pushMessage() {
        ObjectRecord<String, MessageBO> objectRecord = StreamRecords.newRecord()
                .in(Constants.MODEL_RUN_STREAM_KEY)
                .ofObject(build())
                .withId(RecordId.autoGenerate());
        streamRedisTemplate.opsForStream().add(objectRecord);
        return "success";
    }

    private MessageBO build() {
        MessageBO messageBO = new MessageBO();
        messageBO.setId(1);
        messageBO.setMessage("test message");
        return messageBO;
    }

}
