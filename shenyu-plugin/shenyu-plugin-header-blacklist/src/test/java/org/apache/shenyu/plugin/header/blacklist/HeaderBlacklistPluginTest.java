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
import org.apache.shenyu.common.utils.GsonUtils;
import org.apache.shenyu.plugin.api.ShenyuPluginChain;
import org.apache.shenyu.plugin.api.result.DefaultShenyuResult;
import org.apache.shenyu.plugin.api.result.ShenyuResult;
import org.apache.shenyu.plugin.api.utils.SpringBeanUtils;
import org.apache.shenyu.plugin.base.utils.CacheKeyUtils;
import org.apache.shenyu.plugin.header.blacklist.handler.HeaderBlacklistPluginDataHandler;
import org.apache.shenyu.plugin.header.blacklist.rule.HeaderBlacklistHandle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public final class HeaderBlacklistPluginTest {

    private HeaderBlacklistPlugin plugin;

    private ShenyuPluginChain chain;

    @BeforeEach
    public void setUp() {
        ConfigurableApplicationContext context = mock(ConfigurableApplicationContext.class);
        when(context.getBean(ShenyuResult.class)).thenReturn(new DefaultShenyuResult());
        SpringBeanUtils.getInstance().setApplicationContext(context);
        plugin = new HeaderBlacklistPlugin();
        chain = mock(ShenyuPluginChain.class);
        when(chain.execute(any(ServerWebExchange.class))).thenReturn(Mono.empty());
    }

    @Test
    public void testNamedAndOrder() {
        assertEquals("header-blacklist", plugin.named());
        assertEquals(10, plugin.getOrder());
    }

    @Test
    public void testBlacklistRuleReturnsForbidden() {
        RuleData ruleData = buildRule("rule-black", buildHandleJson("ALLOW", Collections.singletonList(buildRuleCondition("X-Token", null, Collections.singletonList("blocked-.*")))));
        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(ruleData),
                GsonUtils.getInstance().fromJson(ruleData.getHandle(), HeaderBlacklistHandle.class));
        ServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("http://localhost/test").header("X-Token", "blocked-value").build());
        Mono<Void> result = plugin.doExecute(exchange, chain, null, ruleData);
        StepVerifier.create(result).verifyComplete();
        assertEquals(HttpStatus.FORBIDDEN, exchange.getResponse().getStatusCode());
        verify(chain, never()).execute(any(ServerWebExchange.class));
    }

    @Test
    public void testWhitelistRulePasses() {
        RuleData ruleData = buildRule("rule-white", buildHandleJson("DENY", Collections.singletonList(buildRuleCondition("X-Token", Collections.singletonList("allowed-.*"), null))));
        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(ruleData),
                GsonUtils.getInstance().fromJson(ruleData.getHandle(), HeaderBlacklistHandle.class));
        ServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("http://localhost/test").header("X-Token", "allowed-value").build());
        Mono<Void> result = plugin.doExecute(exchange, chain, null, ruleData);
        StepVerifier.create(result).verifyComplete();
        verify(chain).execute(exchange);
    }

    @Test
    public void testUnmatchedDefaultDenyReturnsForbidden() {
        RuleData ruleData = buildRule("rule-default-deny", buildHandleJson("DENY", Collections.singletonList(buildRuleCondition("X-Token", Collections.singletonList("allowed-.*"), null))));
        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(ruleData),
                GsonUtils.getInstance().fromJson(ruleData.getHandle(), HeaderBlacklistHandle.class));
        ServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("http://localhost/test").header("X-Token", "other-value").build());
        Mono<Void> result = plugin.doExecute(exchange, chain, null, ruleData);
        StepVerifier.create(result).verifyComplete();
        assertEquals(HttpStatus.FORBIDDEN, exchange.getResponse().getStatusCode());
        verify(chain, never()).execute(any(ServerWebExchange.class));
    }

    @Test
    public void testInvalidRegexReturnsForbidden() {
        RuleData ruleData = buildRule("rule-invalid-regex", buildHandleJson("ALLOW", Collections.singletonList(buildRuleCondition("X-Token", null, Collections.singletonList("[invalid")))));
        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(ruleData),
                GsonUtils.getInstance().fromJson(ruleData.getHandle(), HeaderBlacklistHandle.class));
        ServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("http://localhost/test").header("X-Token", "blocked-value").build());
        Mono<Void> result = plugin.doExecute(exchange, chain, null, ruleData);
        StepVerifier.create(result).verifyComplete();
        assertEquals(HttpStatus.FORBIDDEN, exchange.getResponse().getStatusCode());
        verify(chain, never()).execute(any(ServerWebExchange.class));
    }

    private RuleData buildRule(final String ruleId, final String handleJson) {
        RuleData ruleData = new RuleData();
        ruleData.setId(ruleId);
        ruleData.setSelectorId("selector-" + ruleId);
        ruleData.setHandle(handleJson);
        return ruleData;
    }

    private String buildHandleJson(final String defaultStrategy, final java.util.List<HeaderBlacklistHandle.HeaderRule> headerRules) {
        HeaderBlacklistHandle handle = new HeaderBlacklistHandle();
        handle.setDefaultStrategy(defaultStrategy);
        handle.setHeaderRules(headerRules);
        return GsonUtils.getInstance().toJson(handle);
    }

    private HeaderBlacklistHandle.HeaderRule buildRuleCondition(final String headerName, final java.util.List<String> whitelist,
            final java.util.List<String> blacklist) {
        HeaderBlacklistHandle.HeaderRule headerRule = new HeaderBlacklistHandle.HeaderRule();
        headerRule.setHeaderName(headerName);
        headerRule.setWhitelist(whitelist);
        headerRule.setBlacklist(blacklist);
        return headerRule;
    }
}
