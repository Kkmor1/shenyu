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

package org.apache.shenyu.admin.lock;

import org.apache.shenyu.admin.lock.impl.ForUpdateBackedRegisterExecutionLock;
import org.apache.shenyu.admin.lock.util.RegisterTransactionUtil;
import org.apache.shenyu.admin.mapper.PluginMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionStatus;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class ForUpdateBackedRegisterExecutionLockConcurrentTest {

    private ForUpdateBackedRegisterExecutionLock lock;

    @Mock
    private PlatformTransactionManager transactionManager;

    @Mock
    private PluginMapper pluginMapper;

    @BeforeEach
    public void setup() {
        RegisterTransactionUtil.remove();
        lock = new ForUpdateBackedRegisterExecutionLock(transactionManager, pluginMapper, "testPlugin");
    }

    @Test
    public void testLockFailureCleansUpThreadLocal() {
        TransactionStatus mockTransaction = mock(TransactionStatus.class);
        doReturn(mockTransaction).when(transactionManager).getTransaction(any(DefaultTransactionDefinition.class));
        doThrow(new RuntimeException("DB error")).when(pluginMapper).selectByNameForUpdate("testPlugin");
        doNothing().when(transactionManager).rollback(mockTransaction);

        assertDoesNotThrow(() -> {
            try {
                lock.lock();
            } catch (RuntimeException e) {
                // expected
            }
        });

        assertNull(RegisterTransactionUtil.get(),
                "ThreadLocal should be cleaned up after lock() failure");
        verify(transactionManager, times(1)).rollback(mockTransaction);
    }

    @Test
    public void testUnlockWithNullTransactionStatusIsSafe() {
        RegisterTransactionUtil.remove();
        assertDoesNotThrow(() -> lock.unlock(),
                "unlock() should not throw when ThreadLocal is null");
    }

    @Test
    public void testConcurrentLockUnlockNoThreadLocalLeak() throws Exception {
        final int concurrency = 100;
        AtomicInteger successCount = new AtomicInteger(0);

        doReturn(mock(TransactionStatus.class)).when(transactionManager).getTransaction(any(DefaultTransactionDefinition.class));
        doReturn(null).when(pluginMapper).selectByNameForUpdate("testPlugin");
        doNothing().when(transactionManager).commit(any(TransactionStatus.class));

        ExecutorService executorService = Executors.newFixedThreadPool(16);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(concurrency);

        for (int i = 0; i < concurrency; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    lock.lock();
                    try {
                        successCount.incrementAndGet();
                    } finally {
                        lock.unlock();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(60, TimeUnit.SECONDS);

        assert completed : "All concurrent lock/unlock operations should complete";
        assertNull(RegisterTransactionUtil.get(),
                "ThreadLocal should be clean after all operations");
        assertEquals(concurrency, successCount.get());

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testLockUnlockNormalFlow() {
        DefaultTransactionStatus status = new DefaultTransactionStatus(null, true, true, false, false, null);
        doReturn(status).when(transactionManager).getTransaction(any(DefaultTransactionDefinition.class));
        doReturn(null).when(pluginMapper).selectByNameForUpdate("testPlugin");
        doNothing().when(transactionManager).commit(any(TransactionStatus.class));

        lock.lock();
        lock.unlock();

        assertNull(RegisterTransactionUtil.get(),
                "ThreadLocal should be cleaned up after unlock()");
        verify(transactionManager, times(1)).commit(any(TransactionStatus.class));
    }

    @Test
    public void testLockFailureRollsBackTransaction() {
        TransactionStatus mockTransaction = mock(TransactionStatus.class);
        doReturn(mockTransaction).when(transactionManager).getTransaction(any(DefaultTransactionDefinition.class));
        doThrow(new RuntimeException("For update failed")).when(pluginMapper).selectByNameForUpdate("testPlugin");
        doNothing().when(transactionManager).rollback(mockTransaction);

        try {
            lock.lock();
        } catch (RuntimeException e) {
            // expected
        }

        assertNull(RegisterTransactionUtil.get(),
                "ThreadLocal should be cleaned up even when selectByNameForUpdate throws");
        verify(transactionManager, times(1)).rollback(mockTransaction);
    }
}
