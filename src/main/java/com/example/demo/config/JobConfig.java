package com.example.demo.config;

import com.example.demo.bo.MessageBO;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.hash.ObjectHashMapper;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.example.demo.config.Constants.*;
import static io.lettuce.core.ReadFrom.REPLICA_PREFERRED;


@Configuration
public class JobConfig {

    @Value("${spring.redis.host}")
    private String host;

    @Value("${spring.redis.port}")
    private int port;

    @Value("${spring.redis.password}")
    private String password;

    @Bean
    public GenericJackson2JsonRedisSerializer jsonRedisSerializer(Jackson2ObjectMapperBuilder objectMapperBuilder) {
        return new GenericJackson2JsonRedisSerializer();
    }

    @Bean
    public LettuceConnectionFactory redisConnectionFactory() {
        RedisStandaloneConfiguration serverConfig = new RedisStandaloneConfiguration(host, port);
        serverConfig.setPassword(password);
        LettucePoolingClientConfiguration clientConfig = LettucePoolingClientConfiguration.builder()
                .readFrom(REPLICA_PREFERRED)
                .build();

        return new LettuceConnectionFactory(serverConfig, clientConfig);
    }

    @Bean
    public RedisTemplate<String, Object> streamRedisTemplate(
            RedisConnectionFactory redisConnectionFactory, GenericJackson2JsonRedisSerializer genericJackson2JsonRedisSerializer) {
        RedisTemplate template = new RedisTemplate<String, Object>();
        template.setConnectionFactory(redisConnectionFactory);
        template.setKeySerializer(RedisSerializer.string());
        template.setValueSerializer(genericJackson2JsonRedisSerializer);
        template.setHashKeySerializer(RedisSerializer.string());
        template.setHashValueSerializer(genericJackson2JsonRedisSerializer);
        return template;
    }

    @Bean
    public Executor redisStreamExecutor() {
        int processors = Runtime.getRuntime().availableProcessors();
        AtomicInteger index = new AtomicInteger(1);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(processors, processors, 0, TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(), r -> {
            Thread thread = new Thread(r);
            thread.setName("redisConsumer-" + index.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        });
        return executor;
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public StreamMessageListenerContainer<String, ObjectRecord<String, MessageBO>> streamMessageListenerContainer(Executor redisStreamExecutor,
                                                                                                                  RedisConnectionFactory redisConnectionFactory,
                                                                                                                  RedisTemplate streamRedisTemplate,
                                                                                                                  GenericJackson2JsonRedisSerializer genericJackson2JsonRedisSerializer
    ) {
        try {
            streamRedisTemplate.opsForStream().createGroup(MODEL_RUN_STREAM_KEY, MODEL_RUN_CONSUMER_GROUP);
        } catch (RedisSystemException redisSystemException) {
            //nothing do
        }
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, ObjectRecord<String, MessageBO>> options =
                StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                        .builder()
                        .batchSize(10)
                        .executor(redisStreamExecutor)
                        .keySerializer(RedisSerializer.string())
                        .hashKeySerializer(RedisSerializer.string())
                        .hashValueSerializer(genericJackson2JsonRedisSerializer)
                        // less than `spring.redis.timeout`
                        .pollTimeout(Duration.ofSeconds(1))
                        .objectMapper(new ObjectHashMapper())
                        .targetType(MessageBO.class)
                        .build();
        StreamMessageListenerContainer<String, ObjectRecord<String, MessageBO>> streamMessageListenerContainer =
                StreamMessageListenerContainer.create(redisConnectionFactory, options);
        StreamMessageListenerContainer.ConsumerStreamReadRequest<String> streamReadRequest = StreamMessageListenerContainer
                .StreamReadRequest
                .builder(StreamOffset.create(MODEL_RUN_STREAM_KEY, ReadOffset.lastConsumed()))
                .consumer(Consumer.from(MODEL_RUN_CONSUMER_GROUP, MODEL_RUN_CONSUMER_NAME))
                .autoAcknowledge(false)
                .cancelOnError(throwable -> false)
                .build();
        streamMessageListenerContainer.register(streamReadRequest, new ModelJobConsumerListener(MODEL_RUN_CONSUMER_GROUP, streamRedisTemplate));
        return streamMessageListenerContainer;
    }

}
