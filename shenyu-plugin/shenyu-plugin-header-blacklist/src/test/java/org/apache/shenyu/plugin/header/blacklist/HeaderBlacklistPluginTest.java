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

import org.apache.shenyu.common.dto.PluginData;
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.enums.PluginEnum;
import org.apache.shenyu.plugin.api.ShenyuPluginChain;
import org.apache.shenyu.plugin.api.result.DefaultShenyuResult;
import org.apache.shenyu.plugin.api.result.ShenyuResult;
import org.apache.shenyu.plugin.api.utils.SpringBeanUtils;
import org.apache.shenyu.plugin.base.utils.CacheKeyUtils;
import org.apache.shenyu.plugin.header.blacklist.handle.HeaderBlacklistRuleHandle;
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

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test case for {@link HeaderBlacklistPlugin}.
 */
public final class HeaderBlacklistPluginTest {

    private HeaderBlacklistPlugin headerBlacklistPlugin;

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
        headerBlacklistPlugin = new HeaderBlacklistPlugin();
    }

    @Test
    public void testNamed() {
        final String result = headerBlacklistPlugin.named();
        assertEquals(PluginEnum.HEADER_BLACKLIST.getName(), result);
    }

    @Test
    public void testGetOrder() {
        final int result = headerBlacklistPlugin.getOrder();
        assertEquals(PluginEnum.HEADER_BLACKLIST.getCode(), result);
    }

    @Test
    public void testBlacklistMatchReject() {
        ruleData.setId("rule1");
        ruleData.setSelectorId("selector1");

        List<HeaderBlacklistRuleHandle.HeaderRule> headerRules = new ArrayList<>();
        HeaderBlacklistRuleHandle.HeaderRule rule = new HeaderBlacklistRuleHandle.HeaderRule();
        rule.setHeaderName("User-Agent");
        rule.setPattern(".*BadBot.*");
        headerRules.add(rule);

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setModel("blacklist");
        handle.setHeaderRules(headerRules);

        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(ruleData), handle);

        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("User-Agent", "BadBot/1.0")
                .build());

        Mono<Void> execute = headerBlacklistPlugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
        assertEquals(HttpStatus.FORBIDDEN, exchange.getResponse().getStatusCode());
    }

    @Test
    public void testBlacklistNoMatchPass() {
        ruleData.setId("rule2");
        ruleData.setSelectorId("selector2");

        List<HeaderBlacklistRuleHandle.HeaderRule> headerRules = new ArrayList<>();
        HeaderBlacklistRuleHandle.HeaderRule rule = new HeaderBlacklistRuleHandle.HeaderRule();
        rule.setHeaderName("User-Agent");
        rule.setPattern(".*BadBot.*");
        headerRules.add(rule);

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setModel("blacklist");
        handle.setHeaderRules(headerRules);

        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(ruleData), handle);

        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("User-Agent", "GoodBot/1.0")
                .build());

        Mono<Void> execute = headerBlacklistPlugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
    }

    @Test
    public void testWhitelistMatchPass() {
        ruleData.setId("rule3");
        ruleData.setSelectorId("selector3");

        List<HeaderBlacklistRuleHandle.HeaderRule> headerRules = new ArrayList<>();
        HeaderBlacklistRuleHandle.HeaderRule rule = new HeaderBlacklistRuleHandle.HeaderRule();
        rule.setHeaderName("X-Api-Key");
        rule.setPattern("valid-key-.*");
        headerRules.add(rule);

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setModel("whitelist");
        handle.setHeaderRules(headerRules);

        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(ruleData), handle);

        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("X-Api-Key", "valid-key-12345")
                .build());

        Mono<Void> execute = headerBlacklistPlugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
    }

    @Test
    public void testWhitelistNoMatchReject() {
        ruleData.setId("rule4");
        ruleData.setSelectorId("selector4");

        List<HeaderBlacklistRuleHandle.HeaderRule> headerRules = new ArrayList<>();
        HeaderBlacklistRuleHandle.HeaderRule rule = new HeaderBlacklistRuleHandle.HeaderRule();
        rule.setHeaderName("X-Api-Key");
        rule.setPattern("valid-key-.*");
        headerRules.add(rule);

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setModel("whitelist");
        handle.setHeaderRules(headerRules);

        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(ruleData), handle);

        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("X-Api-Key", "invalid-key-99999")
                .build());

        Mono<Void> execute = headerBlacklistPlugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
        assertEquals(HttpStatus.FORBIDDEN, exchange.getResponse().getStatusCode());
    }

    @Test
    public void testNullRuleHandlePass() {
        Mono<Void> execute = headerBlacklistPlugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
    }

    @Test
    public void testEmptyHeaderRulesPass() {
        ruleData.setId("rule5");
        ruleData.setSelectorId("selector5");

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setModel("blacklist");
        handle.setHeaderRules(new ArrayList<>());

        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(ruleData), handle);

        Mono<Void> execute = headerBlacklistPlugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
    }

    @Test
    public void testNullModelPass() {
        ruleData.setId("rule6");
        ruleData.setSelectorId("selector6");

        List<HeaderBlacklistRuleHandle.HeaderRule> headerRules = new ArrayList<>();
        HeaderBlacklistRuleHandle.HeaderRule rule = new HeaderBlacklistRuleHandle.HeaderRule();
        rule.setHeaderName("User-Agent");
        rule.setPattern(".*BadBot.*");
        headerRules.add(rule);

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setModel(null);
        handle.setHeaderRules(headerRules);

        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(ruleData), handle);

        Mono<Void> execute = headerBlacklistPlugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
    }

    @Test
    public void testInvalidRegexPatternPass() {
        ruleData.setId("rule7");
        ruleData.setSelectorId("selector7");

        List<HeaderBlacklistRuleHandle.HeaderRule> headerRules = new ArrayList<>();
        HeaderBlacklistRuleHandle.HeaderRule rule = new HeaderBlacklistRuleHandle.HeaderRule();
        rule.setHeaderName("User-Agent");
        rule.setPattern("[invalid(regex");
        headerRules.add(rule);

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setModel("blacklist");
        handle.setHeaderRules(headerRules);

        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(ruleData), handle);

        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("User-Agent", "SomeBot/1.0")
                .build());

        Mono<Void> execute = headerBlacklistPlugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
    }

    @Test
    public void testMissingHeaderPass() {
        ruleData.setId("rule8");
        ruleData.setSelectorId("selector8");

        List<HeaderBlacklistRuleHandle.HeaderRule> headerRules = new ArrayList<>();
        HeaderBlacklistRuleHandle.HeaderRule rule = new HeaderBlacklistRuleHandle.HeaderRule();
        rule.setHeaderName("X-Missing-Header");
        rule.setPattern(".*");
        headerRules.add(rule);

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setModel("blacklist");
        handle.setHeaderRules(headerRules);

        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(ruleData), handle);

        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost").build());

        Mono<Void> execute = headerBlacklistPlugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
    }

    @Test
    public void testMultipleRulesAnyMatchReject() {
        ruleData.setId("rule9");
        ruleData.setSelectorId("selector9");

        List<HeaderBlacklistRuleHandle.HeaderRule> headerRules = new ArrayList<>();
        
        HeaderBlacklistRuleHandle.HeaderRule rule1 = new HeaderBlacklistRuleHandle.HeaderRule();
        rule1.setHeaderName("User-Agent");
        rule1.setPattern(".*BadBot.*");
        headerRules.add(rule1);

        HeaderBlacklistRuleHandle.HeaderRule rule2 = new HeaderBlacklistRuleHandle.HeaderRule();
        rule2.setHeaderName("X-Forwarded-For");
        rule2.setPattern("192\\.168\\.1\\.100");
        headerRules.add(rule2);

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setModel("blacklist");
        handle.setHeaderRules(headerRules);

        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(ruleData), handle);

        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("X-Forwarded-For", "192.168.1.100")
                .build());

        Mono<Void> execute = headerBlacklistPlugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
        assertEquals(HttpStatus.FORBIDDEN, exchange.getResponse().getStatusCode());
    }

    @Test
    public void testUnknownModelPass() {
        ruleData.setId("rule10");
        ruleData.setSelectorId("selector10");

        List<HeaderBlacklistRuleHandle.HeaderRule> headerRules = new ArrayList<>();
        HeaderBlacklistRuleHandle.HeaderRule rule = new HeaderBlacklistRuleHandle.HeaderRule();
        rule.setHeaderName("User-Agent");
        rule.setPattern(".*");
        headerRules.add(rule);

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setModel("unknown");
        handle.setHeaderRules(headerRules);

        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(ruleData), handle);

        exchange = MockServerWebExchange.from(MockServerHttpRequest.get("localhost")
                .header("User-Agent", "TestBot/1.0")
                .build());

        Mono<Void> execute = headerBlacklistPlugin.doExecute(exchange, chain, selectorData, ruleData);
        StepVerifier.create(execute).expectSubscription().verifyComplete();
    }
}
