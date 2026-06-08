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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class LocalTokenBucketTest {

    @Test
    public void testTryConsumeSuccess() {
        LocalTokenBucket bucket = new LocalTokenBucket(10.0, 5.0);
        assertTrue(bucket.tryConsume(1));
    }

    @Test
    public void testTryConsumeExceedsBurst() {
        LocalTokenBucket bucket = new LocalTokenBucket(10.0, 3.0);
        assertTrue(bucket.tryConsume(1));
        assertTrue(bucket.tryConsume(1));
        assertTrue(bucket.tryConsume(1));
        assertFalse(bucket.tryConsume(1));
    }

    @Test
    public void testTryConsumeMultipleTokens() {
        LocalTokenBucket bucket = new LocalTokenBucket(10.0, 10.0);
        assertTrue(bucket.tryConsume(5));
        assertFalse(bucket.tryConsume(6));
        assertTrue(bucket.tryConsume(5));
        assertFalse(bucket.tryConsume(1));
    }

    @Test
    public void testTokenRefill() throws InterruptedException {
        LocalTokenBucket bucket = new LocalTokenBucket(1000.0, 5.0);
        for (int i = 0; i < 5; i++) {
            assertTrue(bucket.tryConsume(1));
        }
        assertFalse(bucket.tryConsume(1));
        Thread.sleep(100);
        assertTrue(bucket.tryConsume(1));
    }

    @Test
    public void testBurstCapacityNotExceeded() throws InterruptedException {
        LocalTokenBucket bucket = new LocalTokenBucket(10000.0, 5.0);
        for (int i = 0; i < 5; i++) {
            assertTrue(bucket.tryConsume(1));
        }
        Thread.sleep(100);
        long tokens = bucket.getStoredTokens();
        assertTrue(tokens <= 5, "Tokens should not exceed burst capacity, but was: " + tokens);
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException {
        int burstCapacity = 100;
        int threadCount = 50;
        int requestsPerThread = 10;
        LocalTokenBucket bucket = new LocalTokenBucket(100000.0, burstCapacity);
        CountDownLatch latch = new CountDownLatch(threadCount);
        List<Boolean> results = Collections.synchronizedList(new ArrayList<>());
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < requestsPerThread; j++) {
                        results.add(bucket.tryConsume(1));
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();
        long allowedCount = results.stream().filter(Boolean::booleanValue).count();
        assertEquals(burstCapacity, allowedCount, "Total allowed requests should equal burst capacity");
    }

    @Test
    public void testGetRateAndBurst() {
        LocalTokenBucket bucket = new LocalTokenBucket(10.0, 20.0);
        assertEquals(10.0, bucket.getRate(), 0.001);
        assertEquals(20.0, bucket.getBurst(), 0.001);
    }
}
