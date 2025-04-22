package io.j13n.commons.service;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Service
public class CacheService extends RedisPubSubAdapter<String, String> {

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    private VirtualThreadManager virtualThreadManager;

    @Autowired(required = false)
    private RedisCommands<String, Object> redisCommand;

    @Autowired(required = false)
    @Qualifier("subRedisCommand")
    private RedisPubSubCommands<String, String> subCommand;

    @Autowired(required = false)
    @Qualifier("pubRedisCommand")
    private RedisPubSubCommands<String, String> pubCommand;

    @Autowired(required = false)
    private StatefulRedisPubSubConnection<String, String> subConnect;

    @Value("${spring.application.name}")
    private String appName;

    @Value("${redis.channel:evictionChannel}")
    private String channel;

    @Value("${redis.cache.prefix:unk}")
    private String redisPrefix;

    @Value("${spring.cache.type:}")
    private CacheType cacheType;

    @PostConstruct
    public void registerEviction() {
        if (redisCommand == null || this.cacheType == CacheType.NONE) return;

        subCommand.subscribe(channel);
        subConnect.addListener(this);
    }

    public boolean evict(String cName, String key) {
        if (this.cacheType == CacheType.NONE) return true;

        // Use a virtual thread to perform the actual evict operation
        virtualThreadManager.newVirtualThread(() -> {
            String cacheName = this.redisPrefix + "-" + cName;

            if (pubCommand != null) {
                pubCommand.publish(this.channel, cacheName + ":" + key);
                redisCommand.hdel(cacheName, key);
            } else {
                try {
                    this.caffineCacheEvict(cacheName, key);
                } catch (Exception e) {
                    // Log exception if needed
                }
            }
        });

        // Return success immediately (non-blocking)
        return true;
    }

    // Removed redundant evictAsync methods as evict now uses virtual threads internally

    private boolean caffineCacheEvict(String cacheName, String key) {
        Cache x = cacheManager.getCache(cacheName);
        if (x != null) x.evictIfPresent(key);
        return true;
    }

    public boolean evict(String cacheName, Object... keys) {
        if (this.cacheType == CacheType.NONE) return true;

        String key = makeKey(keys);
        return this.evict(cacheName, key);
    }

    public String makeKey(Object... args) {
        if (args.length == 1) return args[0].toString();

        return Arrays.stream(args)
                .filter(Objects::nonNull)
                .map(Object::toString)
                .collect(Collectors.joining());
    }

    public <T> T put(String cName, T value, Object... keys) {
        if (this.cacheType == CacheType.NONE) return value;

        // Use a virtual thread to perform the actual put operation
        virtualThreadManager.newVirtualThread(() -> {
            String cacheName = this.redisPrefix + "-" + cName;
            String key = makeKey(keys);

            CacheObject co = new CacheObject(value);

            this.cacheManager.getCache(cacheName).put(key, co);

            if (redisCommand != null) redisCommand.hset(cacheName, key, co);
        });

        // Return the value immediately (non-blocking)
        return value;
    }

    // Removed redundant putAsync method as put now uses virtual threads internally

    @SuppressWarnings("unchecked")
    public <T> Optional<T> get(String cName, Object... keys) {
        if (this.cacheType == CacheType.NONE) return Optional.empty();

        String cacheName = this.redisPrefix + "-" + cName;
        String key = makeKey(keys);

        Cache cache = this.cacheManager.getCache(cacheName);
        if (cache == null) return Optional.empty();

        Cache.ValueWrapper wrapper = cache.get(key);
        if (wrapper != null) {
            Object value = wrapper.get();
            if (value instanceof CacheObject co) return Optional.ofNullable((T) co.getObject());
        }

        if (redisCommand != null) {
            Object redisValue = redisCommand.hget(cacheName, key);
            if (redisValue instanceof CacheObject co) return Optional.ofNullable((T) co.getObject());
        }

        return Optional.empty();
    }

    // Note: Unlike put and evict, get is kept as a synchronous operation
    // If asynchronous behavior is needed, use CompletableFuture.supplyAsync(() -> get(cName, keys))
    // or other async patterns

    @SuppressWarnings("unchecked")
    public <T> T cacheValueOrGet(String cName, Supplier<T> supplier, Object... keys) {
        String key = makeKey(keys);
        Optional<T> cachedValue = this.get(cName, key);

        if (cachedValue.isPresent()) return cachedValue.get();

        T value = supplier.get();
        if (value != null) this.put(cName, value, key);

        return value;
    }

    /**
     * Asynchronously retrieves a value from the cache or computes it if not present using a virtual thread.
     * This method is particularly useful for expensive operations that should be executed asynchronously.
     *
     * @param cName the cache name
     * @param supplier the supplier to compute the value if not in cache
     * @param keys the keys to use for the cache entry
     * @param <T> the type of the value
     * @return a Future representing the result of the operation
     */
    public <T> CompletableFuture<T> cacheValueOrGetAsync(String cName, Supplier<T> supplier, Object... keys) {
        CompletableFuture<T> future = new CompletableFuture<>();

        // Create a virtual thread to handle the cache operation
        Thread thread = virtualThreadManager.newVirtualThread(() -> {
            try {
                // First check if the value is in the cache
                String key = makeKey(keys);
                Optional<T> cachedValue = get(cName, key);

                if (cachedValue.isPresent()) {
                    future.complete(cachedValue.get());
                    return;
                }

                // If not in cache, compute the value
                T value = supplier.get();
                if (value != null) {
                    put(cName, value, key);
                }

                future.complete(value);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });

        // The thread is already started by newVirtualThread

        return future;
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<T> cacheEmptyValueOrGet(String cName, Supplier<Optional<T>> supplier, Object... keys) {
        String key = makeKey(keys);
        Optional<T> cachedValue = this.get(cName, key);

        if (cachedValue.isPresent()) return cachedValue;

        // Check if we have a cached null value
        Cache cache = this.cacheManager.getCache(this.redisPrefix + "-" + cName);
        if (cache != null && cache.get(key) != null)
            // We have a cached value (which is null)
            return Optional.empty();

        Optional<T> value = supplier.get();
        if (value.isPresent()) {
            this.put(cName, new CacheObject(value.get()), key);
        } else {
            this.put(cName, new CacheObject(null), key);
        }

        return value;
    }

    /**
     * Asynchronously retrieves an optional value from the cache or computes it if not present using a virtual thread.
     * This method handles both present values and explicitly cached null values.
     *
     * @param cName the cache name
     * @param supplier the supplier to compute the optional value if not in cache
     * @param keys the keys to use for the cache entry
     * @param <T> the type of the value
     * @return a CompletableFuture representing the result of the operation
     */
    public <T> CompletableFuture<Optional<T>> cacheEmptyValueOrGetAsync(
            String cName, Supplier<Optional<T>> supplier, Object... keys) {
        CompletableFuture<Optional<T>> future = new CompletableFuture<>();

        // Create a virtual thread to handle the cache operation
        virtualThreadManager.newVirtualThread(() -> {
            try {
                String key = makeKey(keys);
                Optional<T> cachedValue = get(cName, key);

                if (cachedValue.isPresent()) {
                    future.complete(cachedValue);
                    return;
                }

                // Check if we have a cached null value
                Cache cache = cacheManager.getCache(redisPrefix + "-" + cName);
                if (cache != null && cache.get(key) != null) {
                    // We have a cached value (which is null)
                    future.complete(Optional.empty());
                    return;
                }

                Optional<T> value = supplier.get();
                if (value.isPresent()) {
                    put(cName, new CacheObject(value.get()), key);
                } else {
                    put(cName, new CacheObject(null), key);
                }

                future.complete(value);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });

        return future;
    }

    public boolean evictAll(String cName) {
        if (this.cacheType == CacheType.NONE) return true;

        // Use a virtual thread to perform the actual evictAll operation
        virtualThreadManager.newVirtualThread(() -> {
            String cacheName = this.redisPrefix + "-" + cName;

            if (pubCommand != null) {
                pubCommand.publish(this.channel, cacheName + ":*");
                redisCommand.del(cacheName);
            } else {
                try {
                    this.cacheManager.getCache(cacheName).clear();
                } catch (Exception e) {
                    // Log exception if needed
                }
            }
        });

        // Return success immediately (non-blocking)
        return true;
    }

    // Removed redundant evictAllAsync method as evictAll now uses virtual threads internally

    public boolean evictAllCaches() {
        if (this.cacheType == CacheType.NONE) return true;

        // Use a virtual thread to perform the actual evictAllCaches operation
        virtualThreadManager.newVirtualThread(() -> {
            if (pubCommand != null) {
                List<String> keys = redisCommand.keys(this.redisPrefix + "-*");

                for (String key : keys) {
                    pubCommand.publish(this.channel, key + ":*");
                    redisCommand.del(key);
                }
            } else {
                for (String cacheName : this.cacheManager.getCacheNames()) {
                    Cache cache = this.cacheManager.getCache(cacheName);
                    if (cache != null) {
                        cache.clear();
                    }
                }
            }
        });

        // Return success immediately (non-blocking)
        return true;
    }

    // Removed redundant evictAllCachesAsync method as evictAllCaches now uses virtual threads internally

    /**
     * Asynchronously evicts all values from all caches in parallel using multiple virtual threads.
     * This method creates a separate virtual thread for each cache, which can significantly
     * improve performance when there are many caches.
     *
     * @return a CompletableFuture representing the result of the operation
     */
    public CompletableFuture<Boolean> evictAllCachesParallelAsync() {
        if (this.cacheType == CacheType.NONE) {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            future.complete(true);
            return future;
        }

        CompletableFuture<Boolean> finalResult = new CompletableFuture<>();

        if (pubCommand != null) {
            virtualThreadManager.newVirtualThread(() -> {
                try {
                    List<String> keys = redisCommand.keys(this.redisPrefix + "-*");

                    // Create a future for each key
                    List<CompletableFuture<Boolean>> futures = new ArrayList<>();

                    for (String key : keys) {
                        CompletableFuture<Boolean> keyFuture = new CompletableFuture<>();
                        futures.add(keyFuture);

                        virtualThreadManager.newVirtualThread(() -> {
                            try {
                                pubCommand.publish(this.channel, key + ":*");
                                boolean success = redisCommand.del(key) > 0;
                                keyFuture.complete(success);
                            } catch (Exception e) {
                                keyFuture.completeExceptionally(e);
                            }
                        });
                    }

                    // Wait for all futures to complete
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                            .thenRun(() -> {
                                boolean result = futures.stream()
                                        .map(f -> {
                                            try {
                                                return f.get();
                                            } catch (Exception e) {
                                                return false;
                                            }
                                        })
                                        .reduce(true, (a, b) -> a && b);

                                finalResult.complete(result);
                            })
                            .exceptionally(e -> {
                                finalResult.completeExceptionally(e);
                                return null;
                            });
                } catch (Exception e) {
                    finalResult.completeExceptionally(e);
                }
            });

            return finalResult;
        }

        // For local cache
        virtualThreadManager.newVirtualThread(() -> {
            try {
                Collection<String> cacheNames = cacheManager.getCacheNames();
                List<CompletableFuture<Boolean>> futures = new ArrayList<>();

                for (String cacheName : cacheNames) {
                    CompletableFuture<Boolean> cacheFuture = new CompletableFuture<>();
                    futures.add(cacheFuture);

                    virtualThreadManager.newVirtualThread(() -> {
                        try {
                            Cache cache = cacheManager.getCache(cacheName);
                            if (cache != null) {
                                cache.clear();
                                cacheFuture.complete(true);
                            } else {
                                cacheFuture.complete(false);
                            }
                        } catch (Exception e) {
                            cacheFuture.completeExceptionally(e);
                        }
                    });
                }

                // Wait for all futures to complete
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .thenRun(() -> {
                            boolean result = futures.stream()
                                    .map(f -> {
                                        try {
                                            return f.get();
                                        } catch (Exception e) {
                                            return false;
                                        }
                                    })
                                    .reduce(true, (a, b) -> a && b);

                            finalResult.complete(result);
                        })
                        .exceptionally(e -> {
                            finalResult.completeExceptionally(e);
                            return null;
                        });
            } catch (Exception e) {
                finalResult.completeExceptionally(e);
            }
        });

        return finalResult;
    }

    public Collection<String> getCacheNames() {
        return this.cacheManager.getCacheNames().stream()
                .map(e -> e.substring(this.redisPrefix.length() + 1))
                .toList();
    }

    @Override
    public void message(String channel, String message) {
        if (channel == null || !channel.equals(this.channel)) return;

        int colon = message.indexOf(':');
        if (colon == -1) return;

        String cacheName = message.substring(0, colon);
        String cacheKey = message.substring(colon + 1);

        Cache cache = this.cacheManager.getCache(cacheName);

        if (cache == null) return;

        if (cacheKey.equals("*")) cache.clear();
        else cache.evictIfPresent(cacheKey);
    }

    public <T> Function<T, T> evictAllFunction(String cacheName) {
        return v -> {
            this.evictAll(cacheName);
            return v;
        };
    }

    public <T> Function<T, T> evictFunction(String cacheName, Object... keys) {
        return v -> {
            this.evict(cacheName, keys);
            return v;
        };
    }

    @SuppressWarnings("unchecked")
    public <T> Function<T, T> evictFunctionWithSuppliers(String cacheName, Supplier<Object>... keySuppliers) {
        Object[] keys = new Object[keySuppliers.length];

        for (int i = 0; i < keySuppliers.length; i++) keys[i] = keySuppliers[i].get();

        return v -> {
            this.evict(cacheName, keys);
            return v;
        };
    }

    public <T> Function<T, T> evictFunctionWithKeyFunction(String cacheName, Function<T, String> keyMakingFunction) {
        return v -> {
            this.evict(cacheName, keyMakingFunction.apply(v));
            return v;
        };
    }
}
