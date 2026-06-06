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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * Test cases for {@link MetadataExecutorSubscriber}.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MetadataExecutorSubscriberTest {

    @Mock
    private ShenyuClientRegisterService service;

    @Test
    void testGetType() {
        MetadataExecutorSubscriber metadataExecutorSubscriber = new MetadataExecutorSubscriber(Map.of());
        Assertions.assertEquals(DataType.META_DATA, metadataExecutorSubscriber.getType());
    }

    @Test
    void testExecutor() {
        MetadataExecutorSubscriber metadataExecutorSubscriber = new MetadataExecutorSubscriber(Map.of("http", service));
        List<MetaDataRegisterDTO> list = List.of(MetaDataRegisterDTO.builder().appName("test").rpcType("http").build());
        metadataExecutorSubscriber.executor(list);
        verify(service).register(any());
    }

    @Test
    void testExecutorShouldHandleOneThousandConcurrentRegistrationsWithoutBlockingCallers() throws Exception {
        MetadataExecutorSubscriber metadataExecutorSubscriber = new MetadataExecutorSubscriber(Map.of("http", service));
        int registerCount = 1000;
        CountDownLatch firstRegisterStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstRegister = new CountDownLatch(1);
        CountDownLatch allRegistered = new CountDownLatch(registerCount);
        AtomicBoolean firstInvocation = new AtomicBoolean(true);
        doAnswer(invocation -> {
            if (firstInvocation.compareAndSet(true, false)) {
                firstRegisterStarted.countDown();
                assertTrue(releaseFirstRegister.await(5, TimeUnit.SECONDS));
            }
            allRegistered.countDown();
            return "success";
        }).when(service).register(any(MetaDataRegisterDTO.class));
        ExecutorService executorService = Executors.newFixedThreadPool(16);
        try {
            CompletableFuture<?>[] futures = IntStream.range(0, registerCount)
                    .mapToObj(index -> CompletableFuture.runAsync(() -> metadataExecutorSubscriber.executor(List.of(buildMetaData(index))), executorService))
                    .toArray(CompletableFuture[]::new);
            assertTrue(firstRegisterStarted.await(2, TimeUnit.SECONDS));
            assertDoesNotThrow(() -> CompletableFuture.allOf(futures).get(2, TimeUnit.SECONDS));
            releaseFirstRegister.countDown();
            assertTrue(allRegistered.await(10, TimeUnit.SECONDS));
            verify(service, timeout(10000).times(registerCount)).register(any(MetaDataRegisterDTO.class));
        } finally {
            executorService.shutdownNow();
        }
    }

    private MetaDataRegisterDTO buildMetaData(final int index) {
        return MetaDataRegisterDTO.builder()
                .appName("app-" + index)
                .path("/path-" + index)
                .ruleName("rule-" + index)
                .rpcType("http")
                .namespaceId("default")
                .build();
    }
}
