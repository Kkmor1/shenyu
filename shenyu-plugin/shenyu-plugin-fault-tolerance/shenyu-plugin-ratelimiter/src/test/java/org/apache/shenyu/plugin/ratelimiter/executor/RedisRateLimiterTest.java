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

import com.google.common.collect.Lists;
import org.apache.shenyu.common.dto.convert.rule.RateLimiterHandle;
import org.apache.shenyu.common.utils.Singleton;
import org.apache.shenyu.plugin.ratelimiter.response.RateLimiterResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * RedisRateLimiter test.
 */
@ExtendWith(MockitoExtension.class)
public final class RedisRateLimiterTest {

    private static final String DEFAULT_TEST_ID = "testId";

    private static final double DEFAULT_TEST_REPLENISH_RATE = 1.0;

    private static final double DEFAULT_TEST_BURST_CAPACITY = 300.0;

    private RedisRateLimiter redisRateLimiter;

    private RateLimiterHandle rateLimiterHandle;

    @BeforeEach
    public void setUp() {
        this.redisRateLimiter = new RedisRateLimiter();
        rateLimiterHandle = new RateLimiterHandle();
        rateLimiterHandle.setAlgorithmName("tokenBucket");
        rateLimiterHandle.setReplenishRate(DEFAULT_TEST_REPLENISH_RATE);
        rateLimiterHandle.setBurstCapacity(DEFAULT_TEST_BURST_CAPACITY);
    }

    /**
     * redisRateLimier.isAllowed allowed case for leakyBucketAlgorithm.
     */
    @Test
    public void leakyBucketAllowedTest() {
        leakyBucketPreInit(1L, 10L);
        rateLimiterHandle.setAlgorithmName("leakyBucket");
        Mono<RateLimiterResponse> responseMono = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
        StepVerifier.create(responseMono).assertNext(r -> {
            assertThat(r.getTokensRemaining(), is(10L));
            assertTrue(r.isAllowed());
            assertFalse(r.isLocalFallback());
        }).verifyComplete();
    }

    /**
     * redisRateLimier.isAllowed not allowed case for leakyBucketAlgorithm.
     */
    @Test
    public void leakyBucketNotAllowedTest() {
        leakyBucketPreInit(0L, 300L);
        rateLimiterHandle.setAlgorithmName("leakyBucket");
        Mono<RateLimiterResponse> responseMono = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
        StepVerifier.create(responseMono).assertNext(r -> {
            assertThat(r.getTokensRemaining(), is((long) DEFAULT_TEST_BURST_CAPACITY));
            assertFalse(r.isAllowed());
            assertFalse(r.isLocalFallback());
        }).verifyComplete();
    }

    /**
     * redisRateLimier.isAllowed allowed case for slidingWindowAlgorithm.
     */
    @Test
    public void slidingWindowAllowedTest() {
        slidingWindowPreInit(1L, 200L);
        rateLimiterHandle.setAlgorithmName("slidingWindow");
        Mono<RateLimiterResponse> responseMono = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
        StepVerifier.create(responseMono).assertNext(r -> {
            assertThat(r.getTokensRemaining(), is((long) DEFAULT_TEST_BURST_CAPACITY - 100L));
            assertTrue(r.isAllowed());
            assertFalse(r.isLocalFallback());
        }).verifyComplete();
    }

    /**
     * redisRateLimier.isAllowed not allowed case for slidingWindowAlgorithm.
     */
    @Test
    public void slidingWindowNotAllowedTest() {
        slidingWindowPreInit(0L, 0L);
        rateLimiterHandle.setAlgorithmName("slidingWindow");
        Mono<RateLimiterResponse> responseMono = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
        StepVerifier.create(responseMono).assertNext(r -> {
            assertThat(r.getTokensRemaining(), is((long) DEFAULT_TEST_BURST_CAPACITY - 300L));
            assertFalse(r.isAllowed());
            assertFalse(r.isLocalFallback());
        }).verifyComplete();
    }

    /**
     * redisRateLimiter.isAllowed allowed case.
     */
    @Test
    public void allowedTest() {
        isAllowedPreInit(Flux.just(Lists.newArrayList(1L, 1L)));
        Mono<RateLimiterResponse> responseMono = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
        StepVerifier.create(responseMono).assertNext(r -> {
            assertEquals(1L, r.getTokensRemaining());
            assertTrue(r.isAllowed());
            assertFalse(r.isLocalFallback());
        }).verifyComplete();
    }

    /**
     * redisRateLimiter.isAllowed not allowed case.
     */
    @Test
    public void notAllowedTest() {
        isAllowedPreInit(Flux.just(Lists.newArrayList(0L, 0L)));
        Mono<RateLimiterResponse> responseMono = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
        StepVerifier.create(responseMono).assertNext(r -> {
            assertEquals(0, r.getTokensRemaining());
            assertFalse(r.isAllowed());
            assertFalse(r.isLocalFallback());
        }).verifyComplete();
    }

    /**
     * redisRateLimiter.isAllowed exception case without local fallback.
     */
    @Test
    public void allowedThrowableTest() {
        isAllowedPreInit(Flux.error(new RuntimeException("redis unavailable")));
        Mono<RateLimiterResponse> responseMono = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
        StepVerifier.create(responseMono).verifyError(RuntimeException.class);
    }

    /**
     * redisRateLimiter falls back to local mode when redis unavailable.
     */
    @Test
    public void fallbackToLocalWhenRedisUnavailableTest() {
        rateLimiterHandle.setFallbackToLocal(true);
        rateLimiterHandle.setLocalRate(0);
        rateLimiterHandle.setLocalBurst(1);
        isAllowedPreInit(Flux.error(new RuntimeException("redis unavailable")));

        StepVerifier.create(redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle)).assertNext(r -> {
            assertTrue(r.isAllowed());
            assertEquals(0L, r.getTokensRemaining());
            assertTrue(r.isLocalFallback());
        }).verifyComplete();

        StepVerifier.create(redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle)).assertNext(r -> {
            assertFalse(r.isAllowed());
            assertEquals(0L, r.getTokensRemaining());
            assertTrue(r.isLocalFallback());
        }).verifyComplete();
    }

    /**
     * redisRateLimiter switches back to distributed mode after redis recovery.
     */
    @Test
    public void redisRecoverySwitchBackToDistributedTest() {
        rateLimiterHandle.setFallbackToLocal(true);
        rateLimiterHandle.setLocalRate(0);
        rateLimiterHandle.setLocalBurst(1);
        isAllowedPreInit(
                Flux.error(new RuntimeException("redis unavailable")),
                Flux.just(Lists.newArrayList(1L, 5L))
        );

        StepVerifier.create(redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle)).assertNext(r -> {
            assertTrue(r.isAllowed());
            assertEquals(0L, r.getTokensRemaining());
            assertTrue(r.isLocalFallback());
        }).verifyComplete();

        StepVerifier.create(redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle)).assertNext(r -> {
            assertTrue(r.isAllowed());
            assertEquals(5L, r.getTokensRemaining());
            assertFalse(r.isLocalFallback());
        }).verifyComplete();
    }

    /**
     * redisRateLimiter.isAllowed test pre init.
     *
     * @param fluxes mock lua result flux
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void isAllowedPreInit(final Flux<List<Long>>... fluxes) {
        ReactiveRedisTemplate reactiveRedisTemplate = mock(ReactiveRedisTemplate.class);
        Singleton.INST.single(ReactiveRedisTemplate.class, reactiveRedisTemplate);
        if (fluxes.length == 1) {
            when(reactiveRedisTemplate.execute(any(RedisScript.class), anyList(), anyList())).thenReturn(fluxes[0]);
            return;
        }
        when(reactiveRedisTemplate.execute(any(RedisScript.class), anyList(), anyList()))
                .thenReturn(fluxes[0], Arrays.copyOfRange(fluxes, 1, fluxes.length));
    }

    /**
     * leaky bucket redisRateLimiter.isAllowed test pre init.
     *
     * @param allowFlag mock lua allow result
     * @param waters    mock the waters in the redis
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void leakyBucketPreInit(final long allowFlag, final long waters) {
        ReactiveRedisTemplate reactiveRedisTemplate = mock(ReactiveRedisTemplate.class);
        Singleton.INST.single(ReactiveRedisTemplate.class, reactiveRedisTemplate);
        when(reactiveRedisTemplate.execute(any(RedisScript.class), anyList(), anyList())).thenReturn(
                Flux.just(Lists.newArrayList(allowFlag, waters)));
    }

    /**
     * sliding window redisRateLimiter.isAllowed test pre init.
     *
     * @param allowFlag     mock lua allow result
     * @param remainingNums mock the remain number of the window
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void slidingWindowPreInit(final long allowFlag, final long remainingNums) {
        ReactiveRedisTemplate reactiveRedisTemplate = mock(ReactiveRedisTemplate.class);
        Singleton.INST.single(ReactiveRedisTemplate.class, reactiveRedisTemplate);
        when(reactiveRedisTemplate.execute(any(RedisScript.class), anyList(), anyList())).thenReturn(
                Flux.just(Lists.newArrayList(allowFlag, remainingNums)));
    }
}
