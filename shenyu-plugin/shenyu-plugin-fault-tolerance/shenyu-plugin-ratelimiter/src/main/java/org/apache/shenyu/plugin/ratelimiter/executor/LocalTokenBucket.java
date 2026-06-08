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

import java.util.concurrent.atomic.AtomicLong;

public class LocalTokenBucket {

    private final double rate;

    private final double burst;

    private final AtomicLong storedTokens;

    private final AtomicLong lastRefillTimeNanos;

    public LocalTokenBucket(final double rate, final double burst) {
        this.rate = rate;
        this.burst = burst;
        this.storedTokens = new AtomicLong((long) burst);
        this.lastRefillTimeNanos = new AtomicLong(System.nanoTime());
    }

    public boolean tryConsume(final long tokens) {
        refill();
        while (true) {
            long current = storedTokens.get();
            long consumed = current - tokens;
            if (consumed < 0) {
                return false;
            }
            if (storedTokens.compareAndSet(current, consumed)) {
                return true;
            }
        }
    }

    private void refill() {
        long nowNanos = System.nanoTime();
        while (true) {
            long lastNanos = lastRefillTimeNanos.get();
            long elapsedNanos = nowNanos - lastNanos;
            if (elapsedNanos <= 0) {
                return;
            }
            long newTokens = (long) (elapsedNanos * rate / 1_000_000_000.0);
            if (newTokens <= 0) {
                return;
            }
            if (!lastRefillTimeNanos.compareAndSet(lastNanos, nowNanos)) {
                continue;
            }
            while (true) {
                long current = storedTokens.get();
                long refilled = Math.min(current + newTokens, (long) burst);
                if (storedTokens.compareAndSet(current, refilled)) {
                    break;
                }
            }
            return;
        }
    }

    public long getStoredTokens() {
        return storedTokens.get();
    }

    public double getRate() {
        return rate;
    }

    public double getBurst() {
        return burst;
    }
}
