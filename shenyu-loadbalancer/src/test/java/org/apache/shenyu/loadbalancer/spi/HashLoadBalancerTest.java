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

package org.apache.shenyu.loadbalancer.spi;

import org.apache.shenyu.loadbalancer.entity.LoadBalanceData;
import org.apache.shenyu.loadbalancer.entity.Upstream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The type Hash load balancer test.
 */
public final class HashLoadBalancerTest {

    private Method hash;

    private List<Upstream> hashLoadBalancersOrdered;

    private List<Upstream> hashLoadBalancersDisordered;

    private List<Upstream> hashLoadBalancersReversed;

    private ConcurrentSkipListMap<Long, Upstream> treeMapOrdered;

    private ConcurrentSkipListMap<Long, Upstream> treeMapDisordered;

    private ConcurrentSkipListMap<Long, Upstream> treeMapReversed;

    @BeforeEach
    public void setUp() throws Exception {
        this.hash = HashLoadBalancer.class.getDeclaredMethod("hash", String.class);
        this.hash.setAccessible(true);
        this.hashLoadBalancersOrdered = Stream.of(1, 2, 3)
                .map(weight -> Upstream.builder()
                        .url("upstream-" + weight)
                        .build())
                .collect(Collectors.toList());
        this.hashLoadBalancersDisordered = Stream.of(2, 1, 3)
                .map(weight -> Upstream.builder()
                        .url("upstream-" + weight)
                        .build())
                .collect(Collectors.toList());
        this.hashLoadBalancersReversed = Stream.of(3, 2, 1)
                .map(weight -> Upstream.builder()
                        .url("upstream-" + weight)
                        .build())
                .collect(Collectors.toList());
        this.treeMapOrdered = new ConcurrentSkipListMap<>();
        this.treeMapDisordered = new ConcurrentSkipListMap<>();
        this.treeMapReversed = new ConcurrentSkipListMap<>();
        for (Upstream address : hashLoadBalancersOrdered) {
            for (int i = 0; i < 5; i++) {
                String hashKey = "SHENYU-" + address.getUrl() + "-HASH-" + i;
                Object o = hash.invoke(null, hashKey);
                treeMapOrdered.put(Long.parseLong(o.toString()), address);
            }
        }
        for (Upstream address : hashLoadBalancersReversed) {
            for (int i = 0; i < 5; i++) {
                String hashKey = "SHENYU-" + address.getUrl() + "-HASH-" + i;
                Object o = hash.invoke(null, hashKey);
                treeMapReversed.put(Long.parseLong(o.toString()), address);
            }
        }
        for (Upstream address : hashLoadBalancersDisordered) {
            for (int i = 0; i < 5; i++) {
                String hashKey = "SHENYU-" + address.getUrl() + "-HASH-" + i;
                Object o = hash.invoke(null, hashKey);
                treeMapDisordered.put(Long.parseLong(o.toString()), address);
            }
        }
    }

    /**
     * Hash load balancer test.
     */
    @Test
    public void hashLoadBalanceOrderedWeightTest() throws Exception {
        final HashLoadBalancer hashLoadBalancer = new HashLoadBalancer();
        Assertions.assertNull(hashLoadBalancer.select(null, new LoadBalanceData()));
        final Upstream upstream = hashLoadBalancer.select(hashLoadBalancersOrdered, new LoadBalanceData());
        final Long hashKey = Long.parseLong(hash.invoke(null, "127.0.0.1").toString());
        final SortedMap<Long, Upstream> lastRing = treeMapOrdered.tailMap(hashKey);
        final Upstream assertUp = lastRing.get(lastRing.firstKey());
        assertEquals(assertUp.getUrl(), upstream.getUrl());
    }

    @Test
    public void selectTest() {
        final String ip = "SHENYU-upstream-2-HASH-100";
        LoadBalanceData data = new LoadBalanceData();
        data.setIp(ip);
        final HashLoadBalancer hashLoadBalancer = new HashLoadBalancer();
        Assertions.assertNull(hashLoadBalancer.select(null, new LoadBalanceData()));
        final Upstream upstream = hashLoadBalancer.select(hashLoadBalancersOrdered, data);
        assertEquals(treeMapOrdered.firstEntry().getValue().getUrl(), upstream.getUrl());
    }

    @Test
    public void hashLoadBalanceDisorderedWeightTest() throws Exception {
        final HashLoadBalancer hashLoadBalancer = new HashLoadBalancer();
        final Upstream upstream = hashLoadBalancer.select(hashLoadBalancersDisordered, new LoadBalanceData());
        final Long hashKey = Long.parseLong(hash.invoke(null, "127.0.0.1").toString());
        final SortedMap<Long, Upstream> lastRing = treeMapDisordered.tailMap(hashKey);
        final Upstream assertUp = lastRing.get(lastRing.firstKey());
        assertEquals(assertUp.getUrl(), upstream.getUrl());

    }

    @Test
    public void hashLoadBalanceReversedWeightTest() throws Exception {
        final HashLoadBalancer hashLoadBalancer = new HashLoadBalancer();
        final Upstream divideUpstream = hashLoadBalancer.select(hashLoadBalancersReversed, new LoadBalanceData());
        final Long hashKey = Long.parseLong(hash.invoke(null, "127.0.0.1").toString());
        final SortedMap<Long, Upstream> lastRing = treeMapReversed.tailMap(hashKey);
        final Upstream assertUp = lastRing.get(lastRing.firstKey());
        assertEquals(assertUp.getUrl(), divideUpstream.getUrl());
    }
}
