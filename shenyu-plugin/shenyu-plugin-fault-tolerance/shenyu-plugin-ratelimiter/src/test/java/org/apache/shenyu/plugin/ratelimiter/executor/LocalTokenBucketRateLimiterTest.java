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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LocalTokenBucketRateLimiter test.
 */
public final class LocalTokenBucketRateLimiterTest {

    private LocalTokenBucketRateLimiter localTokenBucketRateLimiter;

    private RateLimiterHandle rateLimiterHandle;

    @BeforeEach
    public void setUp() {
        this.localTokenBucketRateLimiter = new LocalTokenBucketRateLimiter();
        this.rateLimiterHandle = new RateLimiterHandle();
        this.rateLimiterHandle.setLocalRate(1.0);
        this.rateLimiterHandle.setLocalBurst(2.0);
        this.rateLimiterHandle.setRequestCount(1.0);
    }

    @Test
    public void testIsAllowed() {
        // Burst capacity is 2. First two requests should be allowed.
        RateLimiterResponse response1 = localTokenBucketRateLimiter.isAllowed("test-id", rateLimiterHandle);
        assertTrue(response1.isAllowed());
        assertEquals(1, response1.getTokensRemaining());
        assertTrue(response1.isLocal());

        RateLimiterResponse response2 = localTokenBucketRateLimiter.isAllowed("test-id", rateLimiterHandle);
        assertTrue(response2.isAllowed());
        assertEquals(0, response2.getTokensRemaining());
        assertTrue(response2.isLocal());

        // Third request should be rejected as bucket is empty
        RateLimiterResponse response3 = localTokenBucketRateLimiter.isAllowed("test-id", rateLimiterHandle);
        assertFalse(response3.isAllowed());
        assertEquals(0, response3.getTokensRemaining());
        assertTrue(response3.isLocal());
    }
}
