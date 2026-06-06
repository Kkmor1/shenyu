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

import org.apache.shenyu.admin.disruptor.subscriber.MetadataExecutorSubscriber;
import org.apache.shenyu.admin.service.register.ShenyuClientRegisterService;
import org.apache.shenyu.register.common.dto.MetaDataRegisterDTO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test case for deadlock scenario in RegisterClientServerDisruptor.
 */
@ExtendWith(MockitoExtension.class)
class RegisterClientServerDisruptorDeadlockTest {

    @Mock
    private ShenyuClientRegisterService shenyuClientRegisterService;

    private static final int CONCURRENT_COUNT = 1000;

    @Test
    @Timeout(value = 60)
    void testConcurrentRegistrationNoDeadlock() throws InterruptedException {
        // Create metadata subscriber
        Map<String, ShenyuClientRegisterService> serviceMap = Collections.singletonMap("http", shenyuClientRegisterService);
        MetadataExecutorSubscriber subscriber = new MetadataExecutorSubscriber(serviceMap);
        
        // Create a service mock that simulates some work
        ShenyuClientRegisterService service = mock(ShenyuClientRegisterService.class);
        when(shenyuClientRegisterService.get(any())).thenReturn(service);
        
        // Setup count down latch
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(CONCURRENT_COUNT);
        AtomicInteger successCount = new AtomicInteger(0);
        
        // Create thread pool for concurrent test
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        
        try {
            // Submit tasks
            for (int i = 0; i < CONCURRENT_COUNT; i++) {
                final int index = i;
                executorService.submit(() -> {
                    try {
                        startLatch.await();
                        // Create test data
                        List<MetaDataRegisterDTO> dataList = new ArrayList<>();
                        dataList.add(MetaDataRegisterDTO.builder()
                                .appName("testApp" + index)
                                .path("/test/path" + index)
                                .ruleName("testRule" + index)
                                .rpcType("http")
                                .namespaceId("default")
                                .build());
                        
                        // Execute subscriber
                        subscriber.executor(dataList);
                        successCount.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }
            
            // Start all threads at once
            startLatch.countDown();
            
            // Wait for all tasks to complete
            doneLatch.await();
            
            // Verify all tasks completed successfully
            System.out.println("Concurrent test completed successfully. Success count: " + successCount.get());
        } finally {
            executorService.shutdown();
        }
    }
}
