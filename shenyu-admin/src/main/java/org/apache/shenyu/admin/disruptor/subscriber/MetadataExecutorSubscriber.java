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

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The type Metadata executor subscriber.
 */
public class MetadataExecutorSubscriber implements ExecutorTypeSubscriber<MetaDataRegisterDTO> {

    private final Map<String, ShenyuClientRegisterService> shenyuClientRegisterService;

    private final Map<ShenyuClientRegisterService, MetadataRegisterSerialExecutor> serialExecutors = new ConcurrentHashMap<>();

    public MetadataExecutorSubscriber(final Map<String, ShenyuClientRegisterService> shenyuClientRegisterService) {
        this.shenyuClientRegisterService = shenyuClientRegisterService;
    }

    @Override
    public DataType getType() {
        return DataType.META_DATA;
    }

    @Override
    public void executor(final Collection<MetaDataRegisterDTO> metaDataRegisterDTOList) {
        metaDataRegisterDTOList.stream()
                .filter(Objects::nonNull)
                .forEach(meta -> Optional.ofNullable(this.shenyuClientRegisterService.get(meta.getRpcType()))
                        .ifPresent(shenyuClientRegisterService -> serialExecutors
                                .computeIfAbsent(shenyuClientRegisterService, key -> new MetadataRegisterSerialExecutor())
                                .offer(meta, shenyuClientRegisterService)));
    }

    private static final class MetadataRegisterSerialExecutor {

        private final Queue<MetaDataRegisterDTO> queue = new ConcurrentLinkedQueue<>();

        private final AtomicBoolean draining = new AtomicBoolean(false);

        private void offer(final MetaDataRegisterDTO metaDataRegisterDTO, final ShenyuClientRegisterService registerService) {
            queue.offer(metaDataRegisterDTO);
            drain(registerService);
        }

        private void drain(final ShenyuClientRegisterService registerService) {
            if (!draining.compareAndSet(false, true)) {
                return;
            }
            do {
                try {
                    MetaDataRegisterDTO current;
                    while ((current = queue.poll()) != null) {
                        registerService.register(current);
                    }
                } finally {
                    draining.set(false);
                }
            } while (!queue.isEmpty() && draining.compareAndSet(false, true));
        }
    }
}
