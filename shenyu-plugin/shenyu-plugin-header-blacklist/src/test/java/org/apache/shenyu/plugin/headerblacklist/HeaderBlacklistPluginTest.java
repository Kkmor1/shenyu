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

package org.apache.shenyu.plugin.headerblacklist;

import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.dto.convert.rule.HeaderBlacklistHandle;
import org.apache.shenyu.common.enums.PluginEnum;
import org.apache.shenyu.common.utils.GsonUtils;
import org.apache.shenyu.plugin.api.ShenyuPluginChain;
import org.apache.shenyu.plugin.api.result.DefaultShenyuResult;
import org.apache.shenyu.plugin.api.result.ShenyuResult;
import org.apache.shenyu.plugin.api.utils.SpringBeanUtils;
import org.apache.shenyu.plugin.base.utils.CacheKeyUtils;
import org.apache.shenyu.plugin.headerblacklist.handler.HeaderBlacklistPluginDataHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test case for {@link HeaderBlacklistPlugin}.
 */
public final class HeaderBlacklistPluginTest {

    private HeaderBlacklistPlugin headerBlacklistPluginUnderTest;

    private ServerWebExchange exchange;

    private ShenyuPluginChain chain;

    private SelectorData selectorData;

    private RuleData ruleData;

    @BeforeEach
    public void setUp() {
        ConfigurableApplicationContext context = mock(ConfigurableApplicationContext.class);
        when(context.getBean(ShenyuResult.class)).thenReturn(new DefaultShenyuResult());
        SpringBeanUtils springBeanUtils = SpringBeanUtils.getInstance();
        springBeanUtils.setApplicationContext(context);

        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost").build());
        chain = mock(ShenyuPluginChain.class);
        when(this.chain.execute(exchange)).thenReturn(Mono.empty());
        selectorData = mock(SelectorData.class);
        ruleData = mock(RuleData.class);
        headerBlacklistPluginUnderTest = new HeaderBlacklistPlugin();
    }

    @Test
    public void testNamed() {
        final String result = headerBlacklistPluginUnderTest.named();
        assertEquals(PluginEnum.HEADER_BLACKLIST.getName(), result);
    }

    @Test
    public void testGetOrder() {
        final int result = headerBlacklistPluginUnderTest.getOrder();
        assertEquals(PluginEnum.HEADER_BLACKLIST.getCode(), result);
    }

    @Test
    public void testHandleIsNull() {
        Mono<Void> execute = headerBlacklistPluginUnderTest.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
    }

    @Test
    public void testMatchBlacklist() {
        ruleData.setId("headerBlacklist");
        ruleData.setSelectorId("headerBlacklist");

        HeaderBlacklistHandle handle = new HeaderBlacklistHandle();
        HeaderBlacklistHandle.HeaderPattern blacklistPattern =
                new HeaderBlacklistHandle.HeaderPattern("X-Forbidden", ".*");
        handle.setBlacklist(Collections.singletonList(blacklistPattern));
        handle.setDefaultStrategy("allow");

        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get()
                .cachedHandle(CacheKeyUtils.INST.getKey(ruleData), handle);

        HttpHeaders headers = new HttpHeaders();
        headers.add("X-Forbidden", "test");
        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .headers(headers).build());

        Mono<Void> execute = headerBlacklistPluginUnderTest.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
        assertEquals(403, exchange.getResponse().getStatusCode().value());
    }

    @Test
    public void testMatchWhitelist() {
        ruleData.setId("headerBlacklist");
        ruleData.setSelectorId("headerBlacklist");

        HeaderBlacklistHandle handle = new HeaderBlacklistHandle();
        HeaderBlacklistHandle.HeaderPattern whitelistPattern =
                new HeaderBlacklistHandle.HeaderPattern("X-Allowed", ".*");
        handle.setWhitelist(Collections.singletonList(whitelistPattern));
        handle.setDefaultStrategy("deny");

        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get()
                .cachedHandle(CacheKeyUtils.INST.getKey(ruleData), handle);

        HttpHeaders headers = new HttpHeaders();
        headers.add("X-Allowed", "test");
        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .headers(headers).build());

        Mono<Void> execute = headerBlacklistPluginUnderTest.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
    }

    @Test
    public void testDefaultStrategyDeny() {
        ruleData.setId("headerBlacklist");
        ruleData.setSelectorId("headerBlacklist");

        HeaderBlacklistHandle handle = new HeaderBlacklistHandle();
        handle.setDefaultStrategy("deny");

        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get()
                .cachedHandle(CacheKeyUtils.INST.getKey(ruleData), handle);

        Mono<Void> execute = headerBlacklistPluginUnderTest.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
        assertEquals(403, exchange.getResponse().getStatusCode().value());
    }

    @Test
    public void testDefaultStrategyAllow() {
        ruleData.setId("headerBlacklist");
        ruleData.setSelectorId("headerBlacklist");

        HeaderBlacklistHandle handle = new HeaderBlacklistHandle();
        handle.setDefaultStrategy("allow");

        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get()
                .cachedHandle(CacheKeyUtils.INST.getKey(ruleData), handle);

        Mono<Void> execute = headerBlacklistPluginUnderTest.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
    }
}
