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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LocalTokenBucketRateLimiter.
 * Thread-safe local token bucket rate limiter.
 */
public class LocalTokenBucketRateLimiter {

    private static final Logger LOG = LoggerFactory.getLogger(LocalTokenBucketRateLimiter.class);

    private final ConcurrentHashMap<String, TokenBucket> tokenBuckets = new ConcurrentHashMap<>();

    /**
     * Check if request is allowed.
     *
     * @param id          the rule id
     * @param rate        the local rate (tokens per second)
     * @param burst       the local burst capacity
     * @param requestCount the request tokens
     * @return true if allowed, false otherwise
     */
    public boolean isAllowed(final String id, final double rate, final double burst, final double requestCount) {
        TokenBucket bucket = tokenBuckets.computeIfAbsent(id, key -> new TokenBucket(rate, burst));
        return bucket.tryConsume(requestCount);
    }

    /**
     * Token bucket implementation.
     */
    private static class TokenBucket {

        private final double rate;
        private final double capacity;
        private double tokens;
        private long lastRefillTime;
        private final ReentrantLock lock = new ReentrantLock();

        public TokenBucket(final double rate, final double capacity) {
            this.rate = rate;
            this.capacity = capacity;
            this.tokens = capacity;
            this.lastRefillTime = System.currentTimeMillis();
        }

        public boolean tryConsume(final double requestedTokens) {
            lock.lock();
            try {
                refill();
                if (tokens >= requestedTokens) {
                    tokens -= requestedTokens;
                    return true;
                }
                return false;
            } finally {
                lock.unlock();
            }
        }

        private void refill() {
            long now = System.currentTimeMillis();
            long elapsed = now - lastRefillTime;
            if (elapsed > 0) {
                double newTokens = (elapsed * rate) / TimeUnit.SECONDS.toMillis(1);
                tokens = Math.min(capacity, tokens + newTokens);
                lastRefillTime = now;
            }
        }
    }
}
