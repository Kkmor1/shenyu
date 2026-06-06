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

package org.apache.shenyu.admin.disruptor.subscriber;

import org.apache.shenyu.admin.service.register.ShenyuClientRegisterService;
import org.apache.shenyu.register.common.dto.MetaDataRegisterDTO;
import org.apache.shenyu.register.common.type.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class MetadataExecutorSubscriberTest {

    @Mock
    private Map<String, ShenyuClientRegisterService> shenyuClientRegisterService;

    @Test
    public void testGetType() {
        MetadataExecutorSubscriber subscriber = new MetadataExecutorSubscriber(shenyuClientRegisterService);
        Assertions.assertEquals(DataType.META_DATA, subscriber.getType());
    }

    @Test
    public void testExecutorWithEmptyList() {
        MetadataExecutorSubscriber subscriber = new MetadataExecutorSubscriber(shenyuClientRegisterService);
        List<MetaDataRegisterDTO> list = new ArrayList<>();
        subscriber.executor(list);
        Assertions.assertTrue(list.isEmpty());
    }

    @Test
    public void testExecutorWithSingleElement() {
        MetadataExecutorSubscriber subscriber = new MetadataExecutorSubscriber(shenyuClientRegisterService);
        List<MetaDataRegisterDTO> list = new ArrayList<>();
        list.add(MetaDataRegisterDTO.builder().appName("test").rpcType("http").build());
        ShenyuClientRegisterService service = mock(ShenyuClientRegisterService.class);
        when(shenyuClientRegisterService.get(any())).thenReturn(service);
        subscriber.executor(list);
        verify(service).register(any());
    }

    @Test
    public void testConcurrentRegistrationNoDeadlock() throws Exception {
        final int concurrency = 1000;
        Map<String, ShenyuClientRegisterService> serviceMap = new ConcurrentHashMap<>();
        ShenyuClientRegisterService mockService = mock(ShenyuClientRegisterService.class);
        serviceMap.put("http", mockService);

        AtomicInteger concurrentCount = new AtomicInteger(0);
        AtomicInteger maxConcurrentCount = new AtomicInteger(0);
        AtomicInteger totalRegistered = new AtomicInteger(0);

        doAnswer(invocation -> {
            int current = concurrentCount.incrementAndGet();
            maxConcurrentCount.updateAndGet(max -> Math.max(max, current));
            Thread.sleep(10);
            concurrentCount.decrementAndGet();
            totalRegistered.incrementAndGet();
            return "success";
        }).when(mockService).register(any());

        MetadataExecutorSubscriber subscriber = new MetadataExecutorSubscriber(serviceMap);

        ExecutorService executorService = Executors.newFixedThreadPool(64);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(concurrency);

        for (int i = 0; i < concurrency; i++) {
            final int index = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    MetaDataRegisterDTO dto = MetaDataRegisterDTO.builder()
                            .appName("app" + index)
                            .path("/path/" + index)
                            .rpcType("http")
                            .build();
                    subscriber.executor(Collections.singletonList(dto));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(120, TimeUnit.SECONDS);

        Assertions.assertTrue(completed, "All 1000 concurrent registrations should complete within 120 seconds without deadlock");
        Assertions.assertEquals(concurrency, totalRegistered.get(), "All 1000 registrations should be processed");
        Assertions.assertTrue(maxConcurrentCount.get() <= 1, "ReentrantLock(fair=true) should serialize access per rpcType");

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testConcurrentRegistrationMultipleRpcTypesNoDeadlock() throws Exception {
        final int concurrency = 1000;
        Map<String, ShenyuClientRegisterService> serviceMap = new ConcurrentHashMap<>();
        String[] rpcTypes = {"http", "dubbo", "grpc", "websocket"};

        for (String rpcType : rpcTypes) {
            ShenyuClientRegisterService mockService = mock(ShenyuClientRegisterService.class);
            AtomicInteger counter = new AtomicInteger(0);
            doAnswer(invocation -> {
                counter.incrementAndGet();
                Thread.sleep(5);
                return "success";
            }).when(mockService).register(any());
            serviceMap.put(rpcType, mockService);
        }

        MetadataExecutorSubscriber subscriber = new MetadataExecutorSubscriber(serviceMap);

        ExecutorService executorService = Executors.newFixedThreadPool(64);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(concurrency);

        for (int i = 0; i < concurrency; i++) {
            final int index = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    String rpcType = rpcTypes[index % rpcTypes.length];
                    MetaDataRegisterDTO dto = MetaDataRegisterDTO.builder()
                            .appName("app" + index)
                            .path("/path/" + index)
                            .rpcType(rpcType)
                            .build();
                    subscriber.executor(Collections.singletonList(dto));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(120, TimeUnit.SECONDS);

        Assertions.assertTrue(completed, "All 1000 concurrent registrations across multiple rpcTypes should complete without deadlock");

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testLockTimeoutDoesNotBlock() throws Exception {
        Map<String, ShenyuClientRegisterService> serviceMap = new ConcurrentHashMap<>();
        ShenyuClientRegisterService mockService = mock(ShenyuClientRegisterService.class);
        serviceMap.put("http", mockService);

        AtomicInteger callCount = new AtomicInteger(0);
        CountDownLatch blockingLatch = new CountDownLatch(1);

        doAnswer(invocation -> {
            callCount.incrementAndGet();
            if (callCount.get() == 1) {
                blockingLatch.await(60, TimeUnit.SECONDS);
            }
            return "success";
        }).when(mockService).register(any());

        MetadataExecutorSubscriber subscriber = new MetadataExecutorSubscriber(serviceMap);

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        executorService.submit(() -> {
            MetaDataRegisterDTO dto = MetaDataRegisterDTO.builder()
                    .appName("blockingApp")
                    .path("/blocking")
                    .rpcType("http")
                    .build();
            subscriber.executor(Collections.singletonList(dto));
        });

        Thread.sleep(100);

        long startTime = System.currentTimeMillis();
        executorService.submit(() -> {
            MetaDataRegisterDTO dto = MetaDataRegisterDTO.builder()
                    .appName("timeoutApp")
                    .path("/timeout")
                    .rpcType("http")
                    .build();
            subscriber.executor(Collections.singletonList(dto));
        }).get(35, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - startTime;

        Assertions.assertTrue(elapsed >= 29000, "Second registration should wait for lock timeout (30s) before giving up");

        blockingLatch.countDown();
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testReentrantLockPerRpcTypeIsIndependent() throws Exception {
        Map<String, ShenyuClientRegisterService> serviceMap = new ConcurrentHashMap<>();
        ShenyuClientRegisterService httpService = mock(ShenyuClientRegisterService.class);
        ShenyuClientRegisterService dubboService = mock(ShenyuClientRegisterService.class);
        serviceMap.put("http", httpService);
        serviceMap.put("dubbo", dubboService);

        CountDownLatch httpStarted = new CountDownLatch(1);
        CountDownLatch dubboDone = new CountDownLatch(1);

        doAnswer(invocation -> {
            httpStarted.countDown();
            Thread.sleep(2000);
            return "success";
        }).when(httpService).register(any());

        doAnswer(invocation -> {
            dubboDone.countDown();
            return "success";
        }).when(dubboService).register(any());

        MetadataExecutorSubscriber subscriber = new MetadataExecutorSubscriber(serviceMap);

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        executorService.submit(() -> {
            MetaDataRegisterDTO dto = MetaDataRegisterDTO.builder()
                    .appName("httpApp")
                    .path("/http")
                    .rpcType("http")
                    .build();
            subscriber.executor(Collections.singletonList(dto));
        });

        httpStarted.await(5, TimeUnit.SECONDS);

        executorService.submit(() -> {
            MetaDataRegisterDTO dto = MetaDataRegisterDTO.builder()
                    .appName("dubboApp")
                    .path("/dubbo")
                    .rpcType("dubbo")
                    .build();
            subscriber.executor(Collections.singletonList(dto));
        });

        boolean dubboCompleted = dubboDone.await(5, TimeUnit.SECONDS);
        Assertions.assertTrue(dubboCompleted, "Different rpcType registrations should not block each other");

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }
}
