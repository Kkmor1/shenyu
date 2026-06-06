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

package org.apache.shenyu.plugin.header.blacklist;

import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.utils.GsonUtils;
import org.apache.shenyu.plugin.api.ShenyuPluginChain;
import org.apache.shenyu.plugin.api.result.DefaultShenyuResult;
import org.apache.shenyu.plugin.api.result.ShenyuResult;
import org.apache.shenyu.plugin.api.utils.SpringBeanUtils;
import org.apache.shenyu.plugin.base.utils.CacheKeyUtils;
import org.apache.shenyu.plugin.header.blacklist.config.HeaderBlacklistRuleHandle;
import org.apache.shenyu.plugin.header.blacklist.handler.HeaderBlacklistPluginDataHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class HeaderBlacklistPluginTest {

    private HeaderBlacklistPlugin plugin;

    private ServerWebExchange exchange;

    private ShenyuPluginChain chain;

    private SelectorData selectorData;

    private RuleData ruleData;

    @BeforeEach
    public void setUp() {
        ConfigurableApplicationContext context = mock(ConfigurableApplicationContext.class);
        when(context.getBean(ShenyuResult.class)).thenReturn(new DefaultShenyuResult());
        SpringBeanUtils.getInstance().setApplicationContext(context);

        plugin = new HeaderBlacklistPlugin();
        chain = mock(ShenyuPluginChain.class);
        when(chain.execute(any())).thenReturn(Mono.empty());
        selectorData = mock(SelectorData.class);
        ruleData = new RuleData();
        ruleData.setId("testRule");
        ruleData.setSelectorId("testSelector");
        ruleData.setPluginName("headerBlacklist");
    }

    @Test
    public void testNamed() {
        assertEquals("headerBlacklist", plugin.named());
    }

    @Test
    public void testGetOrder() {
        assertEquals(10, plugin.getOrder());
    }

    @Test
    public void testWhitelistMatchPass() {
        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("X-Custom-Header", "test-value")
                .build());

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setWhitelistHeaders(Arrays.asList("X-Custom-.*"));
        handle.setBlacklistHeaders(Collections.emptyList());
        handle.setDefaultPolicy("deny");
        cacheRuleHandle(handle);

        Mono<Void> execute = plugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
    }

    @Test
    public void testWhitelistNoMatchDefaultDeny() {
        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("X-Other-Header", "value")
                .build());

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setWhitelistHeaders(Arrays.asList("X-Custom-.*"));
        handle.setBlacklistHeaders(Collections.emptyList());
        handle.setDefaultPolicy("deny");
        cacheRuleHandle(handle);

        Mono<Void> execute = plugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
        assertEquals(HttpStatus.FORBIDDEN, exchange.getResponse().getStatusCode());
    }

    @Test
    public void testBlacklistMatchForbidden() {
        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("X-Blocked-Header", "malicious")
                .build());

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setWhitelistHeaders(Collections.emptyList());
        handle.setBlacklistHeaders(Arrays.asList("X-Blocked-.*"));
        handle.setDefaultPolicy("allow");
        cacheRuleHandle(handle);

        Mono<Void> execute = plugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
        assertEquals(HttpStatus.FORBIDDEN, exchange.getResponse().getStatusCode());
    }

    @Test
    public void testBlacklistNoMatchPass() {
        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("X-Safe-Header", "value")
                .build());

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setWhitelistHeaders(Collections.emptyList());
        handle.setBlacklistHeaders(Arrays.asList("X-Blocked-.*"));
        handle.setDefaultPolicy("allow");
        cacheRuleHandle(handle);

        Mono<Void> execute = plugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
    }

    @Test
    public void testWhitelistPriorityOverBlacklist() {
        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("X-Allowed", "value")
                .build());

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setWhitelistHeaders(Arrays.asList("X-Allowed"));
        handle.setBlacklistHeaders(Arrays.asList("X-Allowed"));
        handle.setDefaultPolicy("deny");
        cacheRuleHandle(handle);

        Mono<Void> execute = plugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
    }

    @Test
    public void testNullRuleHandlePass() {
        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("X-Any", "value")
                .build());

        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().removeHandle(CacheKeyUtils.INST.getKey(ruleData));

        Mono<Void> execute = plugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
    }

    @Test
    public void testMultipleHeadersBlacklistMatch() {
        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("Accept", "text/html")
                .header("X-Bad-Header", "bad")
                .build());

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setWhitelistHeaders(Collections.emptyList());
        handle.setBlacklistHeaders(Arrays.asList("X-Bad-.*", "X-Evil-.*"));
        handle.setDefaultPolicy("allow");
        cacheRuleHandle(handle);

        Mono<Void> execute = plugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
        assertEquals(HttpStatus.FORBIDDEN, exchange.getResponse().getStatusCode());
    }

    @Test
    public void testMultipleHeadersWhitelistMatch() {
        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("X-Allowed-Header", "value")
                .build());

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setWhitelistHeaders(Arrays.asList("X-Good-.*", "X-Allowed-.*"));
        handle.setBlacklistHeaders(Collections.emptyList());
        handle.setDefaultPolicy("deny");
        cacheRuleHandle(handle);

        Mono<Void> execute = plugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
    }

    @Test
    public void testInvalidRegexPatternHandled() {
        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("X-Header", "value")
                .build());

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setWhitelistHeaders(Collections.emptyList());
        handle.setBlacklistHeaders(Arrays.asList("[invalid"));
        handle.setDefaultPolicy("allow");
        cacheRuleHandle(handle);

        Mono<Void> execute = plugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
    }

    @Test
    public void testEmptyWhitelistAndBlacklistDefaultAllow() {
        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("X-Any", "value")
                .build());

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setWhitelistHeaders(Collections.emptyList());
        handle.setBlacklistHeaders(Collections.emptyList());
        handle.setDefaultPolicy("allow");
        cacheRuleHandle(handle);

        Mono<Void> execute = plugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
    }

    @Test
    public void testEmptyWhitelistAndBlacklistDefaultDeny() {
        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("X-Any", "value")
                .build());

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setWhitelistHeaders(Collections.emptyList());
        handle.setBlacklistHeaders(Collections.emptyList());
        handle.setDefaultPolicy("deny");
        cacheRuleHandle(handle);

        Mono<Void> execute = plugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
        assertEquals(HttpStatus.FORBIDDEN, exchange.getResponse().getStatusCode());
    }

    @Test
    public void testCaseInsensitiveHeaderMatching() {
        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("X-Case-Header", "value")
                .build());

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setWhitelistHeaders(Collections.emptyList());
        handle.setBlacklistHeaders(Arrays.asList("x-case-.*"));
        handle.setDefaultPolicy("allow");
        cacheRuleHandle(handle);

        Mono<Void> execute = plugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
        assertEquals(HttpStatus.FORBIDDEN, exchange.getResponse().getStatusCode());
    }

    private void cacheRuleHandle(final HeaderBlacklistRuleHandle handle) {
        ruleData.setHandle(GsonUtils.getInstance().toJson(handle));
        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get()
                .cachedHandle(CacheKeyUtils.INST.getKey(ruleData), handle);
    }
}