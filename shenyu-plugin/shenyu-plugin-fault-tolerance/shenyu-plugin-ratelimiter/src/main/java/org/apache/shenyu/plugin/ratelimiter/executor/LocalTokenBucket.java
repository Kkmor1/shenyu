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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe local token bucket rate limiter.
 * Used as fallback when Redis is unavailable.
 */
public class LocalTokenBucket {

    private final ConcurrentHashMap<String, TokenBucket> buckets = new ConcurrentHashMap<>();

    /**
     * Try to acquire a token. Returns true if allowed, false if rate limited.
     *
     * @param key           the rate limiter key
     * @param replenishRate tokens per second
     * @param burstCapacity max tokens
     * @return true if allowed
     */
    public boolean isAllowed(final String key, final double replenishRate, final double burstCapacity) {
        TokenBucket bucket = buckets.computeIfAbsent(key, k -> new TokenBucket(replenishRate, burstCapacity));
        bucket.updateRate(replenishRate, burstCapacity);
        return bucket.tryAcquire();
    }

    /**
     * Inner token bucket with thread-safe token acquisition.
     */
    private static final class TokenBucket {

        private final ReentrantLock lock = new ReentrantLock();

        private volatile double rate;

        private volatile double burst;

        private double tokens;

        private long lastRefillTime;

        TokenBucket(final double rate, final double burst) {
            this.rate = rate;
            this.burst = burst;
            this.tokens = burst;
            this.lastRefillTime = System.nanoTime();
        }

        void updateRate(final double newRate, final double newBurst) {
            this.rate = newRate;
            this.burst = newBurst;
        }

        boolean tryAcquire() {
            lock.lock();
            try {
                refill();
                if (tokens >= 1.0) {
                    tokens -= 1.0;
                    return true;
                }
                return false;
            } finally {
                lock.unlock();
            }
        }

        private void refill() {
            long now = System.nanoTime();
            double elapsedSeconds = (now - lastRefillTime) / 1_000_000_000.0;
            double tokensToAdd = elapsedSeconds * rate;
            if (tokensToAdd > 0) {
                tokens = Math.min(burst, tokens + tokensToAdd);
                lastRefillTime = now;
            }
        }
    }
}