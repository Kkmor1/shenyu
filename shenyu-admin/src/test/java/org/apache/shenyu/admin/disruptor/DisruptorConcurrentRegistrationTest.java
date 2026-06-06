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

import org.apache.shenyu.admin.disruptor.executor.RegisterServerConsumerExecutor;
import org.apache.shenyu.admin.disruptor.subscriber.MetadataExecutorSubscriber;
import org.apache.shenyu.admin.service.register.ShenyuClientRegisterService;
import org.apache.shenyu.register.common.dto.MetaDataRegisterDTO;
import org.apache.shenyu.register.common.subsriber.ExecutorTypeSubscriber;
import org.apache.shenyu.register.common.type.DataType;
import org.apache.shenyu.register.common.type.DataTypeParent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class DisruptorConcurrentRegistrationTest {

    @Test
    public void test1000ConcurrentMetadataRegistrationsNoDeadlock() throws Exception {
        final int concurrency = 1000;

        Map<String, ShenyuClientRegisterService> serviceMap = new ConcurrentHashMap<>();
        ShenyuClientRegisterService mockService = mock(ShenyuClientRegisterService.class);
        serviceMap.put("http", mockService);

        AtomicInteger totalRegistered = new AtomicInteger(0);
        AtomicInteger concurrentCount = new AtomicInteger(0);
        AtomicInteger maxConcurrent = new AtomicInteger(0);

        doAnswer(invocation -> {
            int current = concurrentCount.incrementAndGet();
            maxConcurrent.updateAndGet(max -> Math.max(max, current));
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
                            .path("/api/path/" + index)
                            .ruleName("rule" + index)
                            .rpcType("http")
                            .namespaceId("default")
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

        Assertions.assertTrue(completed,
                "1000 concurrent metadata registrations must complete within 120 seconds without deadlock");
        Assertions.assertEquals(concurrency, totalRegistered.get(),
                "All 1000 metadata registrations should be processed successfully");

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testRegisterServerConsumerExecutorConcurrentNoDeadlock() throws Exception {
        final int concurrency = 1000;

        Map<String, ShenyuClientRegisterService> serviceMap = new ConcurrentHashMap<>();
        ShenyuClientRegisterService mockService = mock(ShenyuClientRegisterService.class);
        serviceMap.put("http", mockService);

        AtomicInteger totalRegistered = new AtomicInteger(0);
        doAnswer(invocation -> {
            totalRegistered.incrementAndGet();
            Thread.sleep(5);
            return "success";
        }).when(mockService).register(any());

        ExecutorTypeSubscriber<DataTypeParent> metadataSubscriber =
                (ExecutorTypeSubscriber<DataTypeParent>) (ExecutorTypeSubscriber<?>) new MetadataExecutorSubscriber(serviceMap);

        RegisterServerConsumerExecutor.RegisterServerExecutorFactory factory = new RegisterServerConsumerExecutor.RegisterServerExecutorFactory();
        factory.addSubscribers(metadataSubscriber);

        ExecutorService executorService = Executors.newFixedThreadPool(64);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(concurrency);

        for (int i = 0; i < concurrency; i++) {
            final int index = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    RegisterServerConsumerExecutor executor = (RegisterServerConsumerExecutor) factory.create();
                    MetaDataRegisterDTO dto = MetaDataRegisterDTO.builder()
                            .appName("app" + index)
                            .path("/api/path/" + index)
                            .ruleName("rule" + index)
                            .rpcType("http")
                            .namespaceId("default")
                            .build();
                    List<DataTypeParent> data = new ArrayList<>();
                    data.add(dto);
                    executor.setData(data);
                    executor.run();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(120, TimeUnit.SECONDS);

        Assertions.assertTrue(completed,
                "1000 concurrent RegisterServerConsumerExecutor runs must complete within 120 seconds without deadlock");
        Assertions.assertEquals(concurrency, totalRegistered.get(),
                "All 1000 registrations through RegisterServerConsumerExecutor should be processed");

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testMixedDataTypeConcurrentRegistrationNoDeadlock() throws Exception {
        final int concurrency = 1000;

        Map<String, ShenyuClientRegisterService> serviceMap = new ConcurrentHashMap<>();
        ShenyuClientRegisterService httpService = mock(ShenyuClientRegisterService.class);
        ShenyuClientRegisterService dubboService = mock(ShenyuClientRegisterService.class);
        serviceMap.put("http", httpService);
        serviceMap.put("dubbo", dubboService);

        AtomicInteger httpCount = new AtomicInteger(0);
        AtomicInteger dubboCount = new AtomicInteger(0);

        doAnswer(invocation -> {
            httpCount.incrementAndGet();
            Thread.sleep(5);
            return "success";
        }).when(httpService).register(any());

        doAnswer(invocation -> {
            dubboCount.incrementAndGet();
            Thread.sleep(5);
            return "success";
        }).when(dubboService).register(any());

        MetadataExecutorSubscriber subscriber = new MetadataExecutorSubscriber(serviceMap);

        ExecutorService executorService = Executors.newFixedThreadPool(64);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(concurrency);

        int halfConcurrency = concurrency / 2;
        for (int i = 0; i < concurrency; i++) {
            final int index = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    String rpcType = index < halfConcurrency ? "http" : "dubbo";
                    MetaDataRegisterDTO dto = MetaDataRegisterDTO.builder()
                            .appName("app" + index)
                            .path("/api/" + rpcType + "/path/" + index)
                            .ruleName("rule" + index)
                            .rpcType(rpcType)
                            .namespaceId("default")
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

        Assertions.assertTrue(completed,
                "1000 mixed-type concurrent registrations must complete without deadlock");
        Assertions.assertEquals(halfConcurrency, httpCount.get(),
                "All http registrations should be processed");
        Assertions.assertEquals(concurrency - halfConcurrency, dubboCount.get(),
                "All dubbo registrations should be processed");

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testExceptionHandlerInConsumerExecutor() {
        Map<DataType, ExecutorTypeSubscriber<DataTypeParent>> subscriberMap = new HashMap<>();
        ExecutorTypeSubscriber<DataTypeParent> mockSubscriber = mock(ExecutorTypeSubscriber.class);
        when(mockSubscriber.getType()).thenReturn(DataType.META_DATA);
        subscriberMap.put(DataType.META_DATA, mockSubscriber);

        doAnswer(invocation -> {
            throw new RuntimeException("Simulated registration failure");
        }).when(mockSubscriber).executor(any());

        RegisterServerConsumerExecutor executor = new RegisterServerConsumerExecutor(subscriberMap);

        MetaDataRegisterDTO dto = MetaDataRegisterDTO.builder()
                .appName("testApp")
                .path("/test/path")
                .ruleName("testRule")
                .rpcType("http")
                .namespaceId("default")
                .build();

        List<DataTypeParent> data = new ArrayList<>();
        data.add(dto);
        executor.setData(data);

        Assertions.assertDoesNotThrow(() -> executor.run(),
                "RegisterServerConsumerExecutor.run() should catch exceptions and not propagate them");
    }
}
