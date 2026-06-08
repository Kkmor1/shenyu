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
import org.apache.shenyu.plugin.ratelimiter.response.RateLimiterResponse;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Local token bucket rate limiter.
 */
public class LocalTokenBucketRateLimiter {

    private static final double NANOS_PER_SECOND = 1_000_000_000D;

    private final Map<String, TokenBucket> buckets = new ConcurrentHashMap<>();

    /**
     * Is allowed rate limiter response.
     *
     * @param id the id
     * @param limiterHandle the limiter handle
     * @return the rate limiter response
     */
    public RateLimiterResponse isAllowed(final String id, final RateLimiterHandle limiterHandle) {
        double burstCapacity = limiterHandle.getLocalBurst();
        TokenBucket tokenBucket = buckets.computeIfAbsent(id, key -> new TokenBucket(burstCapacity, System.nanoTime()));
        return tokenBucket.tryAcquire(limiterHandle.getLocalRate(), burstCapacity, limiterHandle.getRequestCount());
    }

    private static final class TokenBucket {

        private double tokens;

        private long lastRefillNanos;

        private TokenBucket(final double initialTokens, final long nowNanos) {
            this.tokens = initialTokens;
            this.lastRefillNanos = nowNanos;
        }

        private synchronized RateLimiterResponse tryAcquire(final double rate, final double burstCapacity, final double requestCount) {
            long nowNanos = System.nanoTime();
            double elapsedSeconds = Math.max(0, nowNanos - lastRefillNanos) / NANOS_PER_SECOND;
            tokens = Math.min(burstCapacity, tokens + elapsedSeconds * rate);
            lastRefillNanos = nowNanos;
            boolean allowed = tokens >= requestCount;
            if (allowed) {
                tokens -= requestCount;
            }
            long tokensRemaining = (long) Math.floor(Math.max(tokens, 0D));
            return new RateLimiterResponse(allowed, tokensRemaining, Collections.emptyList(), true);
        }
    }
}
