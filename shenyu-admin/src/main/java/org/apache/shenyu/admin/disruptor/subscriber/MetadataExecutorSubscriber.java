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
import org.apache.shenyu.register.common.subsriber.ExecutorTypeSubscriber;
import org.apache.shenyu.register.common.type.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The type Metadata executor subscriber.
 */
public class MetadataExecutorSubscriber implements ExecutorTypeSubscriber<MetaDataRegisterDTO> {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataExecutorSubscriber.class);

    private static final long LOCK_TIMEOUT_SECONDS = 30L;

    private final Map<String, ShenyuClientRegisterService> shenyuClientRegisterService;

    private final Map<String, ReentrantLock> rpcTypeLocks = new ConcurrentHashMap<>();

    public MetadataExecutorSubscriber(final Map<String, ShenyuClientRegisterService> shenyuClientRegisterService) {
        this.shenyuClientRegisterService = shenyuClientRegisterService;
    }

    @Override
    public DataType getType() {
        return DataType.META_DATA;
    }

    @Override
    public void executor(final Collection<MetaDataRegisterDTO> metaDataRegisterDTOList) {
        metaDataRegisterDTOList.forEach(meta -> Optional.ofNullable(this.shenyuClientRegisterService.get(meta.getRpcType()))
                .ifPresent(shenyuClientRegisterService -> {
                    ReentrantLock lock = rpcTypeLocks.computeIfAbsent(meta.getRpcType(), k -> new ReentrantLock(true));
                    try {
                        if (lock.tryLock(LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                            try {
                                shenyuClientRegisterService.register(meta);
                            } finally {
                                lock.unlock();
                            }
                        } else {
                            LOG.error("Failed to acquire register lock for rpcType: {} within {} seconds, skipping registration for: {}",
                                    meta.getRpcType(), LOCK_TIMEOUT_SECONDS, meta.getPath());
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOG.error("Interrupted while waiting for register lock on rpcType: {}", meta.getRpcType(), e);
                    }
                }));
    }
}
