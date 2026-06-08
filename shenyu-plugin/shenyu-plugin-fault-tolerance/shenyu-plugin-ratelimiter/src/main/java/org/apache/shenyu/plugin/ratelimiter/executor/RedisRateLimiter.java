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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * RedisRateLimiter.
 */
public class RedisRateLimiter {

    private static final Logger LOG = LoggerFactory.getLogger(RedisRateLimiter.class);

    private final LocalTokenBucketRateLimiter localTokenBucketRateLimiter;

    private final AtomicBoolean localFallbackActive = new AtomicBoolean(false);

    public RedisRateLimiter() {
        this(new LocalTokenBucketRateLimiter());
    }

    RedisRateLimiter(final LocalTokenBucketRateLimiter localTokenBucketRateLimiter) {
        this.localTokenBucketRateLimiter = localTokenBucketRateLimiter;
    }

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
                .map(String::valueOf)
                .collect(Collectors.toList());
        ReactiveRedisTemplate<String, String> reactiveRedisTemplate = Singleton.INST.get(ReactiveRedisTemplate.class);
        if (Objects.isNull(reactiveRedisTemplate)) {
            return handleRedisError(id, limiterHandle, rateLimiterAlgorithm, script, keys, scriptArgs,
                    new IllegalStateException("ReactiveRedisTemplate is not initialized"));
        }
        Flux<List<Long>> resultFlux = reactiveRedisTemplate.execute(script, keys, scriptArgs);
        return resultFlux.reduce(new ArrayList<Long>(), (longs, l) -> {
            longs.addAll(l);
            return longs;
        }).map(results -> {
            recoverDistributedModeIfNecessary();
            boolean allowed = ((Number) results.get(0)).longValue() == 1L;
            long tokensLeft = ((Number) results.get(1)).longValue();
            return new RateLimiterResponse(allowed, tokensLeft, keys);
        }).onErrorResume(throwable -> handleRedisError(id, limiterHandle, rateLimiterAlgorithm, script, keys, scriptArgs, throwable));
    }

    private Mono<RateLimiterResponse> handleRedisError(final String id, final RateLimiterHandle limiterHandle,
                                                       final RateLimiterAlgorithm<?> rateLimiterAlgorithm, final RedisScript<?> script,
                                                       final List<String> keys, final List<String> scriptArgs, final Throwable throwable) {
        rateLimiterAlgorithm.callback(script, keys, scriptArgs);
        LOG.error("Error occurred while judging if user is allowed by RedisRateLimiter:{}", throwable.getMessage());
        if (limiterHandle.isFallbackToLocal()) {
            activateLocalFallbackIfNecessary(throwable);
            return Mono.just(localTokenBucketRateLimiter.isAllowed(id, limiterHandle));
        }
        return Mono.just(new RateLimiterResponse(true, -1L, keys));
    }

    private void activateLocalFallbackIfNecessary(final Throwable throwable) {
        if (localFallbackActive.compareAndSet(false, true)) {
            LOG.warn("Redis is unavailable for rate limiter, switching to local token bucket mode:{}", throwable.getMessage());
        }
    }

    private void recoverDistributedModeIfNecessary() {
        if (localFallbackActive.compareAndSet(true, false)) {
            LOG.info("Redis recovered for rate limiter, switching back to distributed mode");
        }
    }
}
