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

package org.apache.shenyu.admin.disruptor;

import org.apache.shenyu.admin.service.DiscoveryService;
import org.apache.shenyu.admin.service.register.ShenyuClientRegisterService;
import org.apache.shenyu.register.common.dto.MetaDataRegisterDTO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Concurrent test case for {@link RegisterClientServerDisruptorPublisher}
 * to verify that 1000 concurrent registrations do not cause deadlock.
 */
class RegisterClientServerDisruptorPublisherConcurrentTest {

    private static final int TOTAL_REGISTRATIONS = 1000;

    private static final int THREAD_COUNT = 10;

    private static final int REGISTRATIONS_PER_THREAD = TOTAL_REGISTRATIONS / THREAD_COUNT;

    private static final int TIMEOUT_SECONDS = 30;

    private RegisterClientServerDisruptorPublisher publisher;

    private ShenyuClientRegisterService shenyuClientRegisterService;

    private DiscoveryService discoveryService;

    private Map<String, ShenyuClientRegisterService> serviceMap;

    private AtomicInteger registerCounter;

    private CountDownLatch completionLatch;

    @BeforeEach
    void setUp() {
        publisher = RegisterClientServerDisruptorPublisher.getInstance();
        shenyuClientRegisterService = mock(ShenyuClientRegisterService.class);
        discoveryService = mock(DiscoveryService.class);
        serviceMap = new HashMap<>();
        serviceMap.put("http", shenyuClientRegisterService);

        registerCounter = new AtomicInteger(0);
        completionLatch = new CountDownLatch(TOTAL_REGISTRATIONS);

        doAnswer(invocation -> {
            registerCounter.incrementAndGet();
            completionLatch.countDown();
            return "success";
        }).when(shenyuClientRegisterService).register(any(MetaDataRegisterDTO.class));

        when(shenyuClientRegisterService.rpcType()).thenReturn("http");
    }

    @AfterEach
    void tearDown() throws Exception {
        Field providerManageField = RegisterClientServerDisruptorPublisher.class.getDeclaredField("providerManage");
        providerManageField.setAccessible(true);
        Object providerManage = providerManageField.get(publisher);
        if (Objects.nonNull(providerManage)) {
            try {
                publisher.close();
            } catch (Exception ignored) {
            }
        }
        providerManageField.set(publisher, null);
    }

    @Test
    void testConcurrentRegistration1000TimesNoDeadlock() throws Exception {
        publisher.start(serviceMap, discoveryService);

        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIndex = t;
            executorService.submit(() -> {
                for (int i = 0; i < REGISTRATIONS_PER_THREAD; i++) {
                    MetaDataRegisterDTO metaDTO = MetaDataRegisterDTO.builder()
                            .appName("testApp-" + threadIndex + "-" + i)
                            .path("/test/path/" + threadIndex + "/" + i)
                            .ruleName("testRule-" + threadIndex + "-" + i)
                            .rpcType("http")
                            .namespaceId("default")
                            .build();
                    publisher.publish(metaDTO);
                }
            });
        }

        executorService.shutdown();
        assertTrue(executorService.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                "All publish threads should complete within timeout");

        boolean allProcessed = completionLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertTrue(allProcessed, "All " + TOTAL_REGISTRATIONS + " registrations should be processed within timeout, "
                + "but only " + (TOTAL_REGISTRATIONS - completionLatch.getCount()) + " were processed");

        assertEquals(TOTAL_REGISTRATIONS, registerCounter.get(),
                "All registrations should be processed");
    }
}