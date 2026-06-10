/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shenyu.plugin.ratelimiter.executor;

import org.apache.shenyu.common.dto.convert.rule.RateLimiterHandle;
import org.apache.shenyu.common.utils.Singleton;
import org.apache.shenyu.plugin.ratelimiter.algorithm.RateLimiterAlgorithm;
import org.apache.shenyu.plugin.ratelimiter.algorithm.RateLimiterAlgorithmFactory;
import org.apache.shenyu.plugin.ratelimiter.response.RateLimiterResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.time.Instant;

/**
 * RedisRateLimiter.
 */
public class RedisRateLimiter {

    private static final Logger LOG = LoggerFactory.getLogger(RedisRateLimiter.class);

    private final ConcurrentHashMap<String, LocalTokenBucket> localTokenBuckets = new ConcurrentHashMap<>();

    /**
     * Verify using different current limiting algorithm scripts.
     *
     * @param id is rule id
     * @param limiterHandle the limiter handle
     * @return {@code Mono<RateLimiterResponse>} to indicate when request processing is complete
     */
    @SuppressWarnings("unchecked")
    public Mono<RateLimiterResponse> isAllowed(final String id, final RateLimiterHandle limiterHandle) {
        double replenishRate = limiterHandle.getReplenishRate();
        double burstCapacity = limiterHandle.getBurstCapacity();
        double requestCount = limiterHandle.getRequestCount();
        RateLimiterAlgorithm<?> rateLimiterAlgorithm = RateLimiterAlgorithmFactory.newInstance(limiterHandle.getAlgorithmName());
        RedisScript<?> script = rateLimiterAlgorithm.getScript();
        List<String> keys = rateLimiterAlgorithm.getKeys(id);
        List<String> scriptArgs = Stream.of(replenishRate, burstCapacity, Instant.now().getEpochSecond(), requestCount)
                .map(String::valueOf).collect(Collectors.toList());
        return Mono.defer(() -> {
            ReactiveRedisTemplate<String, String> reactiveRedisTemplate = Singleton.INST.get(ReactiveRedisTemplate.class);
            if (Objects.isNull(reactiveRedisTemplate)) {
                return handleRedisException(id, limiterHandle,
                        new IllegalStateException("ReactiveRedisTemplate is not initialized"));
            }
            Flux<List<Long>> resultFlux = reactiveRedisTemplate.execute((RedisScript<List<Long>>) script, keys, scriptArgs);
            return resultFlux.reduce(new ArrayList<Long>(), (longs, result) -> {
                longs.addAll(result);
                return longs;
            }).map(results -> {
                boolean allowed = ((Number) results.get(0)).longValue() == 1L;
                long tokensLeft = ((Number) results.get(1)).longValue();
                return new RateLimiterResponse(allowed, tokensLeft, keys);
            }).onErrorResume(throwable -> handleRedisException(id, limiterHandle, throwable));
        });
    }

    private Mono<RateLimiterResponse> handleRedisException(final String id,
                                                           final RateLimiterHandle limiterHandle,
                                                           final Throwable throwable) {
        if (!limiterHandle.isFallbackToLocal()) {
            LOG.error("Error occurred while judging if user is allowed by RedisRateLimiter:{}", throwable.getMessage());
            return Mono.error(throwable);
        }
        LOG.warn("Redis unavailable for rate limiter key {}, fallback to local mode", id, throwable);
        return Mono.fromSupplier(() -> localRateLimit(id, limiterHandle));
    }

    private RateLimiterResponse localRateLimit(final String id, final RateLimiterHandle limiterHandle) {
        double localRate = limiterHandle.getLocalRate() > 0 ? limiterHandle.getLocalRate() : limiterHandle.getReplenishRate();
        double localBurst = limiterHandle.getLocalBurst() > 0 ? limiterHandle.getLocalBurst() : limiterHandle.getBurstCapacity();
        LocalTokenBucket localTokenBucket = localTokenBuckets.computeIfAbsent(id,
                key -> new LocalTokenBucket(localRate, localBurst));
        LocalRateLimitResult result = localTokenBucket.tryAcquire(localRate, localBurst,
                limiterHandle.getRequestCount(), System.nanoTime());
        return new RateLimiterResponse(result.isAllowed(), result.getTokensRemaining(), Collections.emptyList(), true);
    }

    private static final class LocalTokenBucket {

        private double rate;

        private double burst;

        private double tokens;

        private long lastRefillNanos;

        private LocalTokenBucket(final double rate, final double burst) {
            this.rate = Math.max(rate, 0D);
            this.burst = Math.max(burst, 0D);
            this.tokens = this.burst;
            this.lastRefillNanos = System.nanoTime();
        }

        private synchronized LocalRateLimitResult tryAcquire(final double nextRate, final double nextBurst,
                                                             final double requestCount, final long nowNanos) {
            this.rate = Math.max(nextRate, 0D);
            this.burst = Math.max(nextBurst, 0D);
            refill(nowNanos);
            boolean allowed = tokens >= requestCount;
            if (allowed) {
                tokens -= requestCount;
            }
            return new LocalRateLimitResult(allowed, (long) Math.floor(Math.max(tokens, 0D)));
        }

        private void refill(final long nowNanos) {
            if (nowNanos > lastRefillNanos) {
                double elapsedSeconds = (nowNanos - lastRefillNanos) / 1_000_000_000D;
                tokens = Math.min(burst, tokens + elapsedSeconds * rate);
            }
            tokens = Math.min(tokens, burst);
            lastRefillNanos = nowNanos;
        }
    }

    private static final class LocalRateLimitResult {

        private final boolean allowed;

        private final long tokensRemaining;

        private LocalRateLimitResult(final boolean allowed, final long tokensRemaining) {
            this.allowed = allowed;
            this.tokensRemaining = tokensRemaining;
        }

        private boolean isAllowed() {
            return allowed;
        }

        private long getTokensRemaining() {
            return tokensRemaining;
        }
    }
}
