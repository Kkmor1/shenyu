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
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public final class RedisRateLimiterFallbackTest {

    private static final String DEFAULT_TEST_ID = "testId";

    private RedisRateLimiter redisRateLimiter;

    private RateLimiterHandle rateLimiterHandle;

    @BeforeEach
    public void setUp() {
        this.redisRateLimiter = new RedisRateLimiter();
        rateLimiterHandle = new RateLimiterHandle();
        rateLimiterHandle.setReplenishRate(1.0);
        rateLimiterHandle.setBurstCapacity(300.0);
        rateLimiterHandle.setAlgorithmName("tokenBucket");
    }

    @Test
    public void testRedisNormalAllowed() {
        mockRedisResponse(1L, 299L);
        rateLimiterHandle.setFallbackToLocal(false);
        Mono<RateLimiterResponse> responseMono = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
        StepVerifier.create(responseMono).assertNext(r -> {
            assertTrue(r.isAllowed());
            assertEquals(299L, r.getTokensRemaining());
            assertNotNull(r.getKeys());
        }).verifyComplete();
    }

    @Test
    public void testRedisNormalNotAllowed() {
        mockRedisResponse(0L, 0L);
        rateLimiterHandle.setFallbackToLocal(false);
        Mono<RateLimiterResponse> responseMono = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
        StepVerifier.create(responseMono).assertNext(r -> {
            assertFalse(r.isAllowed());
            assertEquals(0L, r.getTokensRemaining());
            assertNotNull(r.getKeys());
        }).verifyComplete();
    }

    @Test
    public void testRedisExceptionWithoutFallback() {
        mockRedisException();
        rateLimiterHandle.setFallbackToLocal(false);
        Mono<RateLimiterResponse> responseMono = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
        StepVerifier.create(responseMono).assertNext(r -> {
            assertTrue(r.isAllowed());
            assertEquals(-1L, r.getTokensRemaining());
            assertNotNull(r.getKeys());
        }).verifyComplete();
    }

    @Test
    public void testRedisExceptionWithFallbackToLocal() {
        mockRedisException();
        rateLimiterHandle.setFallbackToLocal(true);
        rateLimiterHandle.setLocalRate(10.0);
        rateLimiterHandle.setLocalBurst(5.0);
        Mono<RateLimiterResponse> responseMono = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
        StepVerifier.create(responseMono).assertNext(r -> {
            assertTrue(r.isAllowed());
            assertNull(r.getKeys());
        }).verifyComplete();
    }

    @Test
    public void testFallbackToLocalRateLimiting() {
        mockRedisException();
        rateLimiterHandle.setFallbackToLocal(true);
        rateLimiterHandle.setLocalRate(10.0);
        rateLimiterHandle.setLocalBurst(3.0);
        for (int i = 0; i < 3; i++) {
            Mono<RateLimiterResponse> responseMono = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
            StepVerifier.create(responseMono).assertNext(r -> assertTrue(r.isAllowed())).verifyComplete();
        }
        Mono<RateLimiterResponse> responseMono = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
        StepVerifier.create(responseMono).assertNext(r -> assertFalse(r.isAllowed())).verifyComplete();
    }

    @Test
    public void testFallbackUsesDistributedParamsWhenLocalNotSet() {
        mockRedisException();
        rateLimiterHandle.setFallbackToLocal(true);
        rateLimiterHandle.setReplenishRate(10.0);
        rateLimiterHandle.setBurstCapacity(5.0);
        rateLimiterHandle.setLocalRate(0);
        rateLimiterHandle.setLocalBurst(0);
        for (int i = 0; i < 5; i++) {
            Mono<RateLimiterResponse> responseMono = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
            StepVerifier.create(responseMono).assertNext(r -> assertTrue(r.isAllowed())).verifyComplete();
        }
        Mono<RateLimiterResponse> responseMono = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
        StepVerifier.create(responseMono).assertNext(r -> assertFalse(r.isAllowed())).verifyComplete();
    }

    @Test
    public void testRedisRecoveryAfterFallback() {
        mockRedisException();
        rateLimiterHandle.setFallbackToLocal(true);
        rateLimiterHandle.setLocalRate(10.0);
        rateLimiterHandle.setLocalBurst(5.0);
        Mono<RateLimiterResponse> fallbackResponse = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
        StepVerifier.create(fallbackResponse).assertNext(r -> {
            assertTrue(r.isAllowed());
            assertNull(r.getKeys());
        }).verifyComplete();
        mockRedisResponse(1L, 299L);
        Mono<RateLimiterResponse> recoveredResponse = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
        StepVerifier.create(recoveredResponse).assertNext(r -> {
            assertTrue(r.isAllowed());
            assertEquals(299L, r.getTokensRemaining());
            assertNotNull(r.getKeys());
        }).verifyComplete();
    }

    @Test
    public void testRedisRecoveryNotAllowed() {
        mockRedisException();
        rateLimiterHandle.setFallbackToLocal(true);
        rateLimiterHandle.setLocalRate(10.0);
        rateLimiterHandle.setLocalBurst(5.0);
        Mono<RateLimiterResponse> fallbackResponse = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
        StepVerifier.create(fallbackResponse).assertNext(r -> assertTrue(r.isAllowed())).verifyComplete();
        mockRedisResponse(0L, 0L);
        Mono<RateLimiterResponse> recoveredResponse = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
        StepVerifier.create(recoveredResponse).assertNext(r -> {
            assertFalse(r.isAllowed());
            assertEquals(0L, r.getTokensRemaining());
            assertNotNull(r.getKeys());
        }).verifyComplete();
    }

    @Test
    public void testLocalBucketCreatedOnFallback() {
        mockRedisException();
        rateLimiterHandle.setFallbackToLocal(true);
        rateLimiterHandle.setLocalRate(10.0);
        rateLimiterHandle.setLocalBurst(5.0);
        assertTrue(redisRateLimiter.getLocalBuckets().isEmpty());
        Mono<RateLimiterResponse> responseMono = redisRateLimiter.isAllowed(DEFAULT_TEST_ID, rateLimiterHandle);
        StepVerifier.create(responseMono).assertNext(r -> assertTrue(r.isAllowed())).verifyComplete();
        assertTrue(redisRateLimiter.getLocalBuckets().containsKey(DEFAULT_TEST_ID));
    }

    @Test
    public void testDifferentIdsHaveSeparateBuckets() {
        mockRedisException();
        rateLimiterHandle.setFallbackToLocal(true);
        rateLimiterHandle.setLocalRate(10.0);
        rateLimiterHandle.setLocalBurst(2.0);
        String id1 = "rule-1";
        String id2 = "rule-2";
        Mono<RateLimiterResponse> r1 = redisRateLimiter.isAllowed(id1, rateLimiterHandle);
        StepVerifier.create(r1).assertNext(r -> assertTrue(r.isAllowed())).verifyComplete();
        Mono<RateLimiterResponse> r2 = redisRateLimiter.isAllowed(id2, rateLimiterHandle);
        StepVerifier.create(r2).assertNext(r -> assertTrue(r.isAllowed())).verifyComplete();
        assertTrue(redisRateLimiter.getLocalBuckets().containsKey(id1));
        assertTrue(redisRateLimiter.getLocalBuckets().containsKey(id2));
        assertEquals(2, redisRateLimiter.getLocalBuckets().size());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void mockRedisResponse(final long allowed, final long tokensRemaining) {
        ReactiveRedisTemplate reactiveRedisTemplate = mock(ReactiveRedisTemplate.class);
        Singleton.INST.single(ReactiveRedisTemplate.class, reactiveRedisTemplate);
        when(reactiveRedisTemplate.execute(any(RedisScript.class), anyList(), anyList())).thenReturn(
                Flux.just(Lists.newArrayList(allowed, tokensRemaining)));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void mockRedisException() {
        ReactiveRedisTemplate reactiveRedisTemplate = mock(ReactiveRedisTemplate.class);
        Singleton.INST.single(ReactiveRedisTemplate.class, reactiveRedisTemplate);
        when(reactiveRedisTemplate.execute(any(RedisScript.class), anyList(), anyList())).thenReturn(
                Flux.error(Throwable::new));
    }
}
