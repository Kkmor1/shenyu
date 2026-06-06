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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LocalTokenBucketRateLimiter test.
 */
public final class LocalTokenBucketRateLimiterTest {

    private static final String TEST_ID = "testId";

    @Test
    public void testBasicRateLimiting() {
        LocalTokenBucketRateLimiter limiter = new LocalTokenBucketRateLimiter();
        double rate = 10.0;
        double burst = 10.0;
        
        // First 10 requests should be allowed
        for (int i = 0; i < 10; i++) {
            assertTrue(limiter.isAllowed(TEST_ID, rate, burst, 1.0));
        }
        
        // 11th request should be rejected
        assertFalse(limiter.isAllowed(TEST_ID, rate, burst, 1.0));
    }

    @Test
    public void testTokenRefill() throws InterruptedException {
        LocalTokenBucketRateLimiter limiter = new LocalTokenBucketRateLimiter();
        double rate = 1.0;
        double burst = 2.0;
        
        // First 2 requests allowed
        assertTrue(limiter.isAllowed(TEST_ID, rate, burst, 1.0));
        assertTrue(limiter.isAllowed(TEST_ID, rate, burst, 1.0));
        
        // Third request rejected
        assertFalse(limiter.isAllowed(TEST_ID, rate, burst, 1.0));
        
        // Wait for tokens to refill
        Thread.sleep(1100);
        
        // Should have at least 1 token
        assertTrue(limiter.isAllowed(TEST_ID, rate, burst, 1.0));
    }

    @Test
    public void testDifferentKeys() {
        LocalTokenBucketRateLimiter limiter = new LocalTokenBucketRateLimiter();
        double rate = 5.0;
        double burst = 5.0;
        
        // Each key should have its own bucket
        for (int i = 0; i < 5; i++) {
            assertTrue(limiter.isAllowed("key1", rate, burst, 1.0));
            assertTrue(limiter.isAllowed("key2", rate, burst, 1.0));
        }
        
        // Both keys should now be empty
        assertFalse(limiter.isAllowed("key1", rate, burst, 1.0));
        assertFalse(limiter.isAllowed("key2", rate, burst, 1.0));
    }

    @Test
    public void testConcurrentRateLimiting() throws InterruptedException {
        LocalTokenBucketRateLimiter limiter = new LocalTokenBucketRateLimiter();
        double rate = 100.0;
        double burst = 100.0;
        int threadCount = 10;
        int requestsPerThread = 20;
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger allowedCount = new AtomicInteger(0);
        
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < requestsPerThread; j++) {
                        if (limiter.isAllowed(TEST_ID, rate, burst, 1.0)) {
                            allowedCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await();
        executor.shutdown();
        
        // Should allow exactly burst (100) requests
        assertEquals(100, allowedCount.get());
    }

    @Test
    public void testMultipleTokensPerRequest() {
        LocalTokenBucketRateLimiter limiter = new LocalTokenBucketRateLimiter();
        double rate = 10.0;
        double burst = 10.0;
        
        // Request 5 tokens
        assertTrue(limiter.isAllowed(TEST_ID, rate, burst, 5.0));
        
        // Request 6 tokens should fail (only 5 left)
        assertFalse(limiter.isAllowed(TEST_ID, rate, burst, 6.0));
        
        // Request 5 tokens should work
        assertTrue(limiter.isAllowed(TEST_ID, rate, burst, 5.0));
    }
}
