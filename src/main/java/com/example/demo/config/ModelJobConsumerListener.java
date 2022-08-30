package com.example.demo.config;


import com.example.demo.bo.MessageBO;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;

public class ModelJobConsumerListener implements StreamListener<String, ObjectRecord<String, MessageBO>> {

    private String group;
    private RedisTemplate<String, Object> redisTemplate;

    public ModelJobConsumerListener(String group, RedisTemplate<String, Object> redisTemplate) {
        this.group = group;
        this.redisTemplate = redisTemplate;
    }


    @Override
    public void onMessage(ObjectRecord message) {
        MessageBO messageValue = (MessageBO) message.getValue();
        System.out.println(messageValue);
        redisTemplate.opsForStream().acknowledge(group, message);
    }
}
