package io.j13n.commons.configuration;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.j13n.commons.codec.RedisJSONCodec;
import io.j13n.commons.codec.RedisObjectCodec;
import io.j13n.commons.gson.LocalDateTimeAdapter;
import io.j13n.commons.jackson.CommonsSerializationModule;
import io.j13n.commons.jackson.SortSerializationModule;
import io.j13n.commons.jackson.TupleSerializationModule;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.support.TaskExecutorAdapter;
import org.springframework.data.web.PageableHandlerMethodArgumentResolver;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.Executors;

public abstract class AbstractBaseConfiguration implements WebMvcConfigurer {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractBaseConfiguration.class);

    protected final ObjectMapper objectMapper;

    @Value("${redis.url:}")
    private String redisURL;

    @Value("${redis.codec:object}")
    private String codecType;

    private RedisCodec<String, Object> objectCodec;

    protected AbstractBaseConfiguration(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.initialize();
    }

    protected void initialize() {
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.objectMapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS));
        this.objectMapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_EMPTY, JsonInclude.Include.ALWAYS));
        this.objectMapper.registerModule(new CommonsSerializationModule());
        this.objectMapper.registerModule(new TupleSerializationModule());
        this.objectMapper.registerModule(new SortSerializationModule());

        this.objectCodec = "object".equals(codecType) ? new RedisObjectCodec() : new RedisJSONCodec(this.objectMapper);
    }

    @Bean
    public AsyncTaskExecutor applicationTaskExecutor() {
        return new TaskExecutorAdapter(Executors.newVirtualThreadPerTaskExecutor());
    }

    @Bean
    public Gson makeGson() {

        return new GsonBuilder()
                .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
                .create();
    }

    @Bean
    public MappingJackson2HttpMessageConverter mappingJackson2HttpMessageConverter() {
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setObjectMapper(this.objectMapper);
        return converter;
    }

    protected int getInMemorySize() {
        return 1024 * 1024 * 50; // 50MB
    }

    @Bean
    public PageableHandlerMethodArgumentResolver pageableResolver() {
        return new PageableHandlerMethodArgumentResolver();
    }

    @Bean
    public PasswordEncoder passwordEncoder() throws NoSuchAlgorithmException {
        return new BCryptPasswordEncoder(12, SecureRandom.getInstanceStrong());
    }

    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder) {
        return builder
                .messageConverters(mappingJackson2HttpMessageConverter())
                .build();
    }

    @Bean
    public RedisClient redisClient() {
        if (redisURL == null || redisURL.isBlank())
            return null;

        return RedisClient.create(redisURL);
    }

    @Bean
    public StatefulRedisConnection<String, Object> redisConnection(@Autowired(required = false) RedisClient client) {
        if (client == null)
            return null;

        return client.connect(objectCodec);
    }

    @Bean
    public StatefulRedisPubSubConnection<String, String> redisPubSubConnection(
            @Autowired(required = false) RedisClient client) {
        if (client == null)
            return null;

        return client.connectPubSub();
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOriginPatterns("*")
                .allowedMethods("*")
                .allowedHeaders("*")
                .allowCredentials(true)
                .maxAge(3600);
    }

    @Bean
    public Caffeine<Object, Object> caffeineConfig() {
        return Caffeine.newBuilder()
                .expireAfterAccess(Duration.ofMinutes(5))
                .maximumSize(10_000);
    }

    @Bean
    public CacheManager cacheManager(Caffeine<Object, Object> caffeine) {
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager();
        caffeineCacheManager.setCaffeine(caffeine);
        caffeineCacheManager.setAllowNullValues(false);
        return caffeineCacheManager;
    }
}
