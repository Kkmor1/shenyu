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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LocalTokenBucket test.
 */
public final class LocalTokenBucketTest {

    private static final String TEST_KEY = "testKey";

    @Test
    public void testAllowed() {
        LocalTokenBucket bucket = new LocalTokenBucket();
        assertTrue(bucket.isAllowed(TEST_KEY, 100.0, 100.0));
    }

    @Test
    public void testBurstExhaustion() {
        LocalTokenBucket bucket = new LocalTokenBucket();
        for (int i = 0; i < 10; i++) {
            assertTrue(bucket.isAllowed(TEST_KEY, 1.0, 10.0));
        }
        assertFalse(bucket.isAllowed(TEST_KEY, 1.0, 10.0));
    }

    @Test
    public void testMultipleKeysIsolation() {
        LocalTokenBucket bucket = new LocalTokenBucket();
        for (int i = 0; i < 5; i++) {
            assertTrue(bucket.isAllowed("key1", 1.0, 5.0));
        }
        assertFalse(bucket.isAllowed("key1", 1.0, 5.0));
        for (int i = 0; i < 5; i++) {
            assertTrue(bucket.isAllowed("key2", 1.0, 5.0));
        }
        assertFalse(bucket.isAllowed("key2", 1.0, 5.0));
    }

    @Test
    public void testTokenRefill() throws InterruptedException {
        LocalTokenBucket bucket = new LocalTokenBucket();
        for (int i = 0; i < 5; i++) {
            assertTrue(bucket.isAllowed(TEST_KEY, 10.0, 5.0));
        }
        assertFalse(bucket.isAllowed(TEST_KEY, 10.0, 5.0));
        Thread.sleep(200);
        assertTrue(bucket.isAllowed(TEST_KEY, 10.0, 5.0));
    }
}