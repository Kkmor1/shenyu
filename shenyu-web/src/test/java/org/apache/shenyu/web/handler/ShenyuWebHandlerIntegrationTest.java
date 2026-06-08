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

package org.apache.shenyu.web.handler;

import org.apache.shenyu.common.config.ShenyuConfig;
import org.apache.shenyu.common.constant.Constants;
import org.apache.shenyu.common.dto.ConditionData;
import org.apache.shenyu.common.dto.PluginData;
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.enums.MatchModeEnum;
import org.apache.shenyu.common.enums.OperatorEnum;
import org.apache.shenyu.common.enums.ParamTypeEnum;
import org.apache.shenyu.common.enums.PluginHandlerEventEnum;
import org.apache.shenyu.common.enums.SelectorTypeEnum;
import org.apache.shenyu.plugin.api.ShenyuPlugin;
import org.apache.shenyu.plugin.api.ShenyuPluginChain;
import org.apache.shenyu.plugin.api.context.ShenyuContext;
import org.apache.shenyu.plugin.base.cache.BaseDataCache;
import org.apache.shenyu.plugin.base.cache.CommonPluginDataSubscriber;
import org.apache.shenyu.plugin.base.cache.PluginHandlerEvent;
import org.apache.shenyu.plugin.base.handler.PluginDataHandler;
import org.apache.shenyu.sync.data.api.DiscoveryUpstreamDataSubscriber;
import org.apache.shenyu.web.controller.LocalPluginController;
import org.apache.shenyu.web.loader.ShenyuLoaderService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Integration test for ShenyuWebHandler.
 * Covers plugin chain execution, hot reload, exception handling, and local mode.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ShenyuWebHandlerIntegrationTest {

    private ShenyuWebHandler shenyuWebHandler;

    private BaseDataCache baseDataCache;

    private CommonPluginDataSubscriber pluginDataSubscriber;

    @Mock
    private ApplicationEventPublisher eventPublisher;

    @Mock
    private DiscoveryUpstreamDataSubscriber discoveryUpstreamDataSubscriber;

    @BeforeEach
    public void setUp() {
        baseDataCache = BaseDataCache.getInstance();
        baseDataCache.cleanPluginData();
        baseDataCache.cleanSelectorData();
        baseDataCache.cleanRuleData();

        List<PluginDataHandler> pluginDataHandlers = new ArrayList<>();
        pluginDataSubscriber = new CommonPluginDataSubscriber(pluginDataHandlers, eventPublisher,
                new ShenyuConfig.SelectorMatchCache(), new ShenyuConfig.RuleMatchCache());

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();

        List<ShenyuPlugin> plugins = new ArrayList<>();
        shenyuWebHandler = new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);
    }

    @AfterEach
    public void tearDown() {
        baseDataCache.cleanPluginData();
        baseDataCache.cleanSelectorData();
        baseDataCache.cleanRuleData();
    }

    @Test
    public void testPluginChainExecutionOrder() {
        AtomicInteger executionOrder = new AtomicInteger(0);
        List<String> executionSequence = Collections.synchronizedList(new ArrayList<>());

        ShenyuPlugin plugin1 = createOrderedPlugin("plugin-first", 1, (exchange, chain) -> {
            executionSequence.add("plugin-first");
            return chain.execute(exchange);
        });

        ShenyuPlugin plugin2 = createOrderedPlugin("plugin-second", 2, (exchange, chain) -> {
            executionSequence.add("plugin-second");
            return chain.execute(exchange);
        });

        ShenyuPlugin plugin3 = createOrderedPlugin("plugin-third", 3, (exchange, chain) -> {
            executionSequence.add("plugin-third");
            return chain.execute(exchange);
        });

        List<ShenyuPlugin> plugins = new ArrayList<>();
        plugins.add(plugin1);
        plugins.add(plugin2);
        plugins.add(plugin3);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);

        ServerWebExchange exchange = createMockExchange("/test/path");

        Mono<Void> result = handler.handle(exchange);
        result.block();

        assertEquals(3, executionSequence.size());
        assertEquals("plugin-first", executionSequence.get(0));
        assertEquals("plugin-second", executionSequence.get(1));
        assertEquals("plugin-third", executionSequence.get(2));
    }

    @Test
    public void testPluginChainExecutionOrderBySort() {
        List<String> executionSequence = Collections.synchronizedList(new ArrayList<>());

        ShenyuPlugin pluginA = createOrderedPlugin("plugin-a", 100, (exchange, chain) -> {
            executionSequence.add("plugin-a");
            return chain.execute(exchange);
        });

        ShenyuPlugin pluginB = createOrderedPlugin("plugin-b", 50, (exchange, chain) -> {
            executionSequence.add("plugin-b");
            return chain.execute(exchange);
        });

        ShenyuPlugin pluginC = createOrderedPlugin("plugin-c", 200, (exchange, chain) -> {
            executionSequence.add("plugin-c");
            return chain.execute(exchange);
        });

        List<ShenyuPlugin> plugins = new ArrayList<>();
        plugins.add(pluginA);
        plugins.add(pluginB);
        plugins.add(pluginC);

        PluginData pluginDataA = PluginData.builder()
                .id("1")
                .name("plugin-a")
                .enabled(true)
                .sort(100)
                .build();
        PluginData pluginDataB = PluginData.builder()
                .id("2")
                .name("plugin-b")
                .enabled(true)
                .sort(50)
                .build();
        PluginData pluginDataC = PluginData.builder()
                .id("3")
                .name("plugin-c")
                .enabled(true)
                .sort(200)
                .build();

        baseDataCache.cachePluginData(pluginDataA);
        baseDataCache.cachePluginData(pluginDataB);
        baseDataCache.cachePluginData(pluginDataC);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);

        ServerWebExchange exchange = createMockExchange("/test/path");

        Mono<Void> result = handler.handle(exchange);
        result.block();

        assertEquals(3, executionSequence.size());
        assertEquals("plugin-b", executionSequence.get(0));
        assertEquals("plugin-a", executionSequence.get(1));
        assertEquals("plugin-c", executionSequence.get(2));
    }

    @Test
    public void testMonoEmptySkipsSubsequentPlugins() {
        List<String> executionSequence = Collections.synchronizedList(new ArrayList<>());

        ShenyuPlugin plugin1 = createOrderedPlugin("plugin-first", 1, (exchange, chain) -> {
            executionSequence.add("plugin-first");
            return chain.execute(exchange);
        });

        ShenyuPlugin pluginShortCircuit = createOrderedPlugin("plugin-short-circuit", 2, (exchange, chain) -> {
            executionSequence.add("plugin-short-circuit");
            return Mono.empty();
        });

        ShenyuPlugin plugin3 = createOrderedPlugin("plugin-third", 3, (exchange, chain) -> {
            executionSequence.add("plugin-third");
            return chain.execute(exchange);
        });

        List<ShenyuPlugin> plugins = new ArrayList<>();
        plugins.add(plugin1);
        plugins.add(pluginShortCircuit);
        plugins.add(plugin3);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);

        ServerWebExchange exchange = createMockExchange("/test/path");

        Mono<Void> result = handler.handle(exchange);
        result.block();

        assertEquals(2, executionSequence.size());
        assertEquals("plugin-first", executionSequence.get(0));
        assertEquals("plugin-short-circuit", executionSequence.get(1));
        assertFalse(executionSequence.contains("plugin-third"));
    }

    @Test
    public void testPluginSkipMechanism() {
        List<String> executionSequence = Collections.synchronizedList(new ArrayList<>());

        ShenyuPlugin plugin1 = createOrderedPlugin("plugin-first", 1, (exchange, chain) -> {
            executionSequence.add("plugin-first");
            return chain.execute(exchange);
        });

        ShenyuPlugin pluginSkip = createSkipPlugin("plugin-skip", 2, (exchange, chain) -> {
            executionSequence.add("plugin-skip");
            return chain.execute(exchange);
        });

        ShenyuPlugin plugin3 = createOrderedPlugin("plugin-third", 3, (exchange, chain) -> {
            executionSequence.add("plugin-third");
            return chain.execute(exchange);
        });

        List<ShenyuPlugin> plugins = new ArrayList<>();
        plugins.add(plugin1);
        plugins.add(pluginSkip);
        plugins.add(plugin3);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);

        ServerWebExchange exchange = createMockExchange("/test/path");

        Mono<Void> result = handler.handle(exchange);
        result.block();

        assertEquals(2, executionSequence.size());
        assertEquals("plugin-first", executionSequence.get(0));
        assertEquals("plugin-third", executionSequence.get(1));
        assertFalse(executionSequence.contains("plugin-skip"));
    }

    @Test
    public void testPluginHotReload() throws InterruptedException {
        List<String> executionSequence = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean newPluginExecuted = new AtomicBoolean(false);

        ShenyuPlugin plugin1 = createOrderedPlugin("plugin-first", 1, (exchange, chain) -> {
            executionSequence.add("plugin-first");
            return chain.execute(exchange);
        });

        ShenyuPlugin plugin2 = createOrderedPlugin("plugin-second", 2, (exchange, chain) -> {
            executionSequence.add("plugin-second");
            return chain.execute(exchange);
        });

        List<ShenyuPlugin> initialPlugins = new ArrayList<>();
        initialPlugins.add(plugin1);
        initialPlugins.add(plugin2);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(initialPlugins, shenyuLoaderService, shenyuConfig);

        ServerWebExchange exchange1 = createMockExchange("/test/path/1");
        Mono<Void> result1 = handler.handle(exchange1);
        result1.block();

        assertEquals(2, executionSequence.size());
        assertEquals("plugin-first", executionSequence.get(0));
        assertEquals("plugin-second", executionSequence.get(1));

        executionSequence.clear();

        ShenyuPlugin newPlugin = createOrderedPlugin("plugin-new", 3, (exchange, chain) -> {
            executionSequence.add("plugin-new");
            newPluginExecuted.set(true);
            return chain.execute(exchange);
        });

        List<ShenyuPlugin> extPlugins = new ArrayList<>();
        extPlugins.add(newPlugin);
        handler.putExtPlugins(extPlugins);

        ServerWebExchange exchange2 = createMockExchange("/test/path/2");
        Mono<Void> result2 = handler.handle(exchange2);
        result2.block();

        assertTrue(newPluginExecuted.get());
        assertTrue(executionSequence.contains("plugin-new"));
        assertEquals(3, executionSequence.size());
    }

    @Test
    public void testPluginHotReloadReplaceExistingPlugin() {
        AtomicInteger originalPluginCount = new AtomicInteger(0);
        AtomicInteger replacedPluginCount = new AtomicInteger(0);

        ShenyuPlugin originalPlugin = createOrderedPlugin("plugin-test", 1, (exchange, chain) -> {
            originalPluginCount.incrementAndGet();
            return chain.execute(exchange);
        });

        List<ShenyuPlugin> initialPlugins = new ArrayList<>();
        initialPlugins.add(originalPlugin);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(initialPlugins, shenyuLoaderService, shenyuConfig);

        ServerWebExchange exchange1 = createMockExchange("/test/path/1");
        Mono<Void> result1 = handler.handle(exchange1);
        result1.block();

        assertEquals(1, originalPluginCount.get());

        ShenyuPlugin replacedPlugin = createOrderedPlugin("plugin-test", 1, (exchange, chain) -> {
            replacedPluginCount.incrementAndGet();
            return chain.execute(exchange);
        });

        List<ShenyuPlugin> extPlugins = new ArrayList<>();
        extPlugins.add(replacedPlugin);
        handler.putExtPlugins(extPlugins);

        originalPluginCount.set(0);

        ServerWebExchange exchange2 = createMockExchange("/test/path/2");
        Mono<Void> result2 = handler.handle(exchange2);
        result2.block();

        assertEquals(0, originalPluginCount.get());
        assertEquals(1, replacedPluginCount.get());
    }

    @Test
    public void testGlobalExceptionHandlerForIllegalArgumentException() {
        ShenyuPlugin pluginWithException = createOrderedPlugin("plugin-exception", 1, (exchange, chain) -> {
            throw new IllegalArgumentException("Test illegal argument exception");
        });

        List<ShenyuPlugin> plugins = new ArrayList<>();
        plugins.add(pluginWithException);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);

        GlobalErrorHandler errorHandler = new GlobalErrorHandler();

        ServerWebExchange exchange = createMockExchange("/test/error");

        Mono<Void> result = handler.handle(exchange);

        try {
            result.block();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertEquals("Test illegal argument exception", e.getCause().getMessage());
        }
    }

    @Test
    public void testGlobalExceptionHandlerForRuntimeException() {
        ShenyuPlugin pluginWithException = createOrderedPlugin("plugin-runtime-exception", 1, (exchange, chain) -> {
            throw new RuntimeException("Test runtime exception");
        });

        List<ShenyuPlugin> plugins = new ArrayList<>();
        plugins.add(pluginWithException);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);

        ServerWebExchange exchange = createMockExchange("/test/error");

        Mono<Void> result = handler.handle(exchange);

        try {
            result.block();
        } catch (Exception e) {
            assertNotNull(e.getCause());
            assertTrue(e.getCause() instanceof RuntimeException);
        }
    }

    @Test
    public void testGlobalExceptionHandlerWithErrorHandler() {
        ShenyuPlugin pluginWithException = createOrderedPlugin("plugin-error", 1, (exchange, chain) -> {
            throw new RuntimeException("Test error handling");
        });

        List<ShenyuPlugin> plugins = new ArrayList<>();
        plugins.add(pluginWithException);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);

        GlobalErrorHandler errorHandler = new GlobalErrorHandler();

        AtomicReference<Throwable> capturedException = new AtomicReference<>();
        AtomicBoolean errorHandled = new AtomicBoolean(false);

        ServerWebExchange exchange = createMockExchange("/test/error");

        Mono<Void> result = handler.handle(exchange)
                .onErrorResume(throwable -> {
                    capturedException.set(throwable);
                    errorHandled.set(true);
                    return errorHandler.handle(exchange, throwable);
                });

        result.block();

        assertTrue(errorHandled.get());
        assertNotNull(capturedException.get());
    }

    @Test
    public void testLocalPluginControllerUpdatePluginChain() {
        List<String> executionSequence = Collections.synchronizedList(new ArrayList<>());

        ShenyuPlugin testPlugin = createOrderedPlugin("divide", 1, (exchange, chain) -> {
            executionSequence.add("divide");
            PluginData pluginData = baseDataCache.obtainPluginData("divide");
            if (pluginData != null && pluginData.getEnabled()) {
                executionSequence.add("divide-enabled");
            }
            return chain.execute(exchange);
        });

        List<ShenyuPlugin> plugins = new ArrayList<>();
        plugins.add(testPlugin);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);

        ServerWebExchange exchange1 = createMockExchange("/test/before-update");
        Mono<Void> result1 = handler.handle(exchange1);
        result1.block();

        assertTrue(executionSequence.contains("divide"));

        executionSequence.clear();

        PluginData pluginData = PluginData.builder()
                .id("test-divide-id")
                .name("divide")
                .enabled(true)
                .sort(1)
                .config("{\"test\":\"config\"}")
                .build();
        pluginDataSubscriber.onSubscribe(pluginData);

        SelectorData selectorData = SelectorData.builder()
                .id("selector-1")
                .name("test-selector")
                .pluginName("divide")
                .type(SelectorTypeEnum.FULL_FLOW.getCode())
                .matchMode(MatchModeEnum.AND.getCode())
                .enabled(true)
                .sort(1)
                .conditionList(Collections.singletonList(createConditionData()))
                .build();
        pluginDataSubscriber.onSelectorSubscribe(selectorData);

        RuleData ruleData = RuleData.builder()
                .id("rule-1")
                .name("test-rule")
                .selectorId("selector-1")
                .pluginName("divide")
                .matchMode(MatchModeEnum.AND.getCode())
                .enabled(true)
                .sort(1)
                .conditionDataList(Collections.singletonList(createConditionData()))
                .build();
        pluginDataSubscriber.onRuleSubscribe(ruleData);

        ServerWebExchange exchange2 = createMockExchange("/test/after-update");
        Mono<Void> result2 = handler.handle(exchange2);
        result2.block();

        assertTrue(executionSequence.contains("divide"));
        assertTrue(executionSequence.contains("divide-enabled"));

        PluginData cachedPlugin = baseDataCache.obtainPluginData("divide");
        assertNotNull(cachedPlugin);
        assertEquals("divide", cachedPlugin.getName());
        assertTrue(cachedPlugin.getEnabled());
    }

    @Test
    public void testLocalPluginControllerCleanAll() {
        PluginData pluginData = PluginData.builder()
                .id("test-clean-id")
                .name("test-clean-plugin")
                .enabled(true)
                .sort(1)
                .build();
        pluginDataSubscriber.onSubscribe(pluginData);

        assertNotNull(baseDataCache.obtainPluginData("test-clean-plugin"));

        pluginDataSubscriber.refreshPluginDataAll();

        assertNull(baseDataCache.obtainPluginData("test-clean-plugin"));
    }

    @Test
    public void testLocalPluginControllerDeletePlugin() {
        PluginData pluginData = PluginData.builder()
                .id("test-delete-id")
                .name("test-delete-plugin")
                .enabled(true)
                .sort(1)
                .build();
        pluginDataSubscriber.onSubscribe(pluginData);

        assertNotNull(baseDataCache.obtainPluginData("test-delete-plugin"));

        PluginData deletePluginData = PluginData.builder()
                .name("test-delete-plugin")
                .build();
        pluginDataSubscriber.unSubscribe(deletePluginData);

        assertNull(baseDataCache.obtainPluginData("test-delete-plugin"));
    }

    @Test
    public void testPluginEnabledDisabledEvent() {
        List<String> executionSequence = Collections.synchronizedList(new ArrayList<>());

        ShenyuPlugin testPlugin = createOrderedPlugin("test-event-plugin", 1, (exchange, chain) -> {
            executionSequence.add("test-event-plugin");
            return chain.execute(exchange);
        });

        List<ShenyuPlugin> plugins = new ArrayList<>();
        plugins.add(testPlugin);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);

        PluginData pluginData = PluginData.builder()
                .id("test-event-id")
                .name("test-event-plugin")
                .enabled(true)
                .sort(1)
                .build();

        handler.onApplicationEvent(new PluginHandlerEvent(PluginHandlerEventEnum.ENABLED, pluginData));

        ServerWebExchange exchange = createMockExchange("/test/event");
        Mono<Void> result = handler.handle(exchange);
        result.block();

        assertTrue(executionSequence.contains("test-event-plugin"));

        executionSequence.clear();

        PluginData disabledPluginData = PluginData.builder()
                .id("test-event-id")
                .name("test-event-plugin")
                .enabled(false)
                .sort(1)
                .build();

        handler.onApplicationEvent(new PluginHandlerEvent(PluginHandlerEventEnum.DISABLED, disabledPluginData));

        ServerWebExchange exchange2 = createMockExchange("/test/event2");
        Mono<Void> result2 = handler.handle(exchange2);
        result2.block();

        assertFalse(executionSequence.contains("test-event-plugin"));
    }

    @Test
    public void testPluginChainWithDataCache() {
        List<String> executionSequence = Collections.synchronizedList(new ArrayList<>());

        ShenyuPlugin cachePlugin = createOrderedPlugin("cache-plugin", 1, (exchange, chain) -> {
            executionSequence.add("cache-plugin");

            PluginData pluginData = baseDataCache.obtainPluginData("cache-plugin");
            if (pluginData != null) {
                exchange.getAttributes().put("plugin-config", pluginData.getConfig());
            }

            return chain.execute(exchange);
        });

        List<ShenyuPlugin> plugins = new ArrayList<>();
        plugins.add(cachePlugin);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);

        PluginData pluginData = PluginData.builder()
                .id("cache-plugin-id")
                .name("cache-plugin")
                .enabled(true)
                .sort(1)
                .config("{\"cache\":\"enabled\"}")
                .build();
        baseDataCache.cachePluginData(pluginData);

        ServerWebExchange exchange = createMockExchange("/test/cache");
        Mono<Void> result = handler.handle(exchange);
        result.block();

        assertTrue(executionSequence.contains("cache-plugin"));
        assertEquals("{\"cache\":\"enabled\"}", exchange.getAttribute("plugin-config"));
    }

    @Test
    public void testScheduledPluginExecution() {
        AtomicInteger executionCount = new AtomicInteger(0);

        ShenyuPlugin scheduledPlugin = createOrderedPlugin("scheduled-plugin", 1, (exchange, chain) -> {
            executionCount.incrementAndGet();
            return chain.execute(exchange);
        });

        List<ShenyuPlugin> plugins = new ArrayList<>();
        plugins.add(scheduledPlugin);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        shenyuConfig.getScheduler().setEnabled(true);
        shenyuConfig.getScheduler().setType("fixed");
        shenyuConfig.getScheduler().setThreads(4);

        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);

        ServerWebExchange exchange = createMockExchange("/test/scheduled");
        Mono<Void> result = handler.handle(exchange);
        result.block();

        assertEquals(1, executionCount.get());
    }

    @Test
    public void testMultiplePluginsWithDifferentOrders() {
        Map<String, Integer> executionOrderMap = new ConcurrentHashMap<>();
        AtomicInteger counter = new AtomicInteger(0);

        ShenyuPlugin plugin50 = createOrderedPlugin("plugin-50", 50, (exchange, chain) -> {
            executionOrderMap.put("plugin-50", counter.incrementAndGet());
            return chain.execute(exchange);
        });

        ShenyuPlugin plugin10 = createOrderedPlugin("plugin-10", 10, (exchange, chain) -> {
            executionOrderMap.put("plugin-10", counter.incrementAndGet());
            return chain.execute(exchange);
        });

        ShenyuPlugin plugin30 = createOrderedPlugin("plugin-30", 30, (exchange, chain) -> {
            executionOrderMap.put("plugin-30", counter.incrementAndGet());
            return chain.execute(exchange);
        });

        ShenyuPlugin plugin20 = createOrderedPlugin("plugin-20", 20, (exchange, chain) -> {
            executionOrderMap.put("plugin-20", counter.incrementAndGet());
            return chain.execute(exchange);
        });

        List<ShenyuPlugin> plugins = new ArrayList<>();
        plugins.add(plugin50);
        plugins.add(plugin10);
        plugins.add(plugin30);
        plugins.add(plugin20);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);

        ServerWebExchange exchange = createMockExchange("/test/order");
        Mono<Void> result = handler.handle(exchange);
        result.block();

        assertEquals(4, executionOrderMap.size());
        assertTrue(executionOrderMap.get("plugin-10") < executionOrderMap.get("plugin-20"));
        assertTrue(executionOrderMap.get("plugin-20") < executionOrderMap.get("plugin-30"));
        assertTrue(executionOrderMap.get("plugin-30") < executionOrderMap.get("plugin-50"));
    }

    @Test
    public void testPluginChainWithLocalPluginControllerSelectorAndRule() {
        List<String> executionSequence = Collections.synchronizedList(new ArrayList<>());

        ShenyuPlugin testPlugin = createOrderedPlugin("local-test-plugin", 1, (exchange, chain) -> {
            executionSequence.add("local-test-plugin");

            List<SelectorData> selectors = baseDataCache.obtainSelectorData("local-test-plugin");
            if (selectors != null && !selectors.isEmpty()) {
                executionSequence.add("selector-found");
            }

            return chain.execute(exchange);
        });

        List<ShenyuPlugin> plugins = new ArrayList<>();
        plugins.add(testPlugin);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);

        PluginData pluginData = PluginData.builder()
                .id("local-plugin-id")
                .name("local-test-plugin")
                .enabled(true)
                .sort(1)
                .build();
        pluginDataSubscriber.onSubscribe(pluginData);

        SelectorData selectorData = SelectorData.builder()
                .id("local-selector-id")
                .name("local-selector")
                .pluginName("local-test-plugin")
                .type(SelectorTypeEnum.FULL_FLOW.getCode())
                .matchMode(MatchModeEnum.AND.getCode())
                .enabled(true)
                .sort(1)
                .build();
        pluginDataSubscriber.onSelectorSubscribe(selectorData);

        RuleData ruleData = RuleData.builder()
                .id("local-rule-id")
                .name("local-rule")
                .selectorId("local-selector-id")
                .pluginName("local-test-plugin")
                .matchMode(MatchModeEnum.AND.getCode())
                .enabled(true)
                .sort(1)
                .build();
        pluginDataSubscriber.onRuleSubscribe(ruleData);

        ServerWebExchange exchange = createMockExchange("/test/local");
        Mono<Void> result = handler.handle(exchange);
        result.block();

        assertTrue(executionSequence.contains("local-test-plugin"));
        assertTrue(executionSequence.contains("selector-found"));

        List<SelectorData> cachedSelectors = baseDataCache.obtainSelectorData("local-test-plugin");
        assertNotNull(cachedSelectors);
        assertFalse(cachedSelectors.isEmpty());
        assertEquals("local-selector-id", cachedSelectors.get(0).getId());

        List<RuleData> cachedRules = baseDataCache.obtainRuleData("local-selector-id");
        assertNotNull(cachedRules);
        assertFalse(cachedRules.isEmpty());
        assertEquals("local-rule-id", cachedRules.get(0).getId());
    }

    @Test
    public void testPluginChainBeforeAndAfterCallbacks() {
        AtomicBoolean beforeCalled = new AtomicBoolean(false);
        AtomicBoolean afterCalled = new AtomicBoolean(false);
        AtomicReference<Long> chainStartTime = new AtomicReference<>(0L);

        ShenyuPlugin testPlugin = createOrderedPlugin("callback-plugin", 1, (exchange, chain) -> {
            beforeCalled.set(exchange.getAttribute(Constants.CHAIN_START_TIME) != null);
            chainStartTime.set(exchange.getAttribute(Constants.CHAIN_START_TIME));
            return chain.execute(exchange);
        });

        List<ShenyuPlugin> plugins = new ArrayList<>();
        plugins.add(testPlugin);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);

        ServerWebExchange exchange = createMockExchange("/test/callback");
        Mono<Void> result = handler.handle(exchange);
        result.block();

        assertTrue(beforeCalled.get());
        assertTrue(chainStartTime.get() > 0);
    }

    @Test
    public void testPluginChainWithEmptyPluginList() {
        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(new ArrayList<>(), shenyuLoaderService, shenyuConfig);

        ServerWebExchange exchange = createMockExchange("/test/empty");
        Mono<Void> result = handler.handle(exchange);

        assertNotNull(result);
    }

    @Test
    public void testGetPlugins() {
        ShenyuPlugin plugin1 = createOrderedPlugin("get-plugin-1", 1, (exchange, chain) -> chain.execute(exchange));
        ShenyuPlugin plugin2 = createOrderedPlugin("get-plugin-2", 2, (exchange, chain) -> chain.execute(exchange));

        List<ShenyuPlugin> plugins = new ArrayList<>();
        plugins.add(plugin1);
        plugins.add(plugin2);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);

        List<ShenyuPlugin> retrievedPlugins = handler.getPlugins();
        assertNotNull(retrievedPlugins);
        assertEquals(2, retrievedPlugins.size());
    }

    @Test
    public void testPutExtPluginsWithEmptyList() {
        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(new ArrayList<>(), shenyuLoaderService, shenyuConfig);

        handler.putExtPlugins(null);
        handler.putExtPlugins(Collections.emptyList());

        List<ShenyuPlugin> plugins = handler.getPlugins();
        assertNotNull(plugins);
        assertTrue(plugins.isEmpty());
    }

    @Test
    public void testPluginChainWithResponseStatusException() {
        ShenyuPlugin pluginWithResponseStatusException = createOrderedPlugin("plugin-response-status", 1, (exchange, chain) -> {
            throw new org.springframework.web.server.ResponseStatusException(HttpStatus.NOT_FOUND, "Resource not found");
        });

        List<ShenyuPlugin> plugins = new ArrayList<>();
        plugins.add(pluginWithResponseStatusException);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);

        GlobalErrorHandler errorHandler = new GlobalErrorHandler();

        ServerWebExchange exchange = createMockExchange("/test/not-found");

        Mono<Void> result = handler.handle(exchange)
                .onErrorResume(throwable -> errorHandler.handle(exchange, throwable));

        result.block();

        assertEquals(HttpStatus.NOT_FOUND, exchange.getResponse().getStatusCode());
    }

    @Test
    public void testPluginChainConcurrency() throws InterruptedException {
        AtomicInteger concurrentExecutionCount = new AtomicInteger(0);
        AtomicInteger maxConcurrentCount = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(10);

        ShenyuPlugin concurrentPlugin = createOrderedPlugin("concurrent-plugin", 1, (exchange, chain) -> {
            int current = concurrentExecutionCount.incrementAndGet();
            maxConcurrentCount.updateAndGet(max -> Math.max(max, current));
            try {
                startLatch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            concurrentExecutionCount.decrementAndGet();
            endLatch.countDown();
            return chain.execute(exchange);
        });

        List<ShenyuPlugin> plugins = new ArrayList<>();
        plugins.add(concurrentPlugin);

        ShenyuLoaderService shenyuLoaderService = mock(ShenyuLoaderService.class);
        ShenyuConfig shenyuConfig = new ShenyuConfig();
        shenyuConfig.getScheduler().setEnabled(true);
        shenyuConfig.getScheduler().setType("fixed");
        shenyuConfig.getScheduler().setThreads(10);

        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);

        for (int i = 0; i < 10; i++) {
            ServerWebExchange exchange = createMockExchange("/test/concurrent/" + i);
            Mono<Void> result = handler.handle(exchange);
            result.subscribe();
        }

        startLatch.countDown();
        endLatch.await(10, TimeUnit.SECONDS);

        assertTrue(maxConcurrentCount.get() > 0);
    }

    private ShenyuPlugin createOrderedPlugin(String name, int order, PluginExecutor executor) {
        return new ShenyuPlugin() {
            @Override
            public Mono<Void> execute(ServerWebExchange exchange, ShenyuPluginChain chain) {
                return executor.execute(exchange, chain);
            }

            @Override
            public int getOrder() {
                return order;
            }

            @Override
            public String named() {
                return name;
            }

            @Override
            public boolean skip(ServerWebExchange exchange) {
                return false;
            }
        };
    }

    private ShenyuPlugin createSkipPlugin(String name, int order, PluginExecutor executor) {
        return new ShenyuPlugin() {
            @Override
            public Mono<Void> execute(ServerWebExchange exchange, ShenyuPluginChain chain) {
                return executor.execute(exchange, chain);
            }

            @Override
            public int getOrder() {
                return order;
            }

            @Override
            public String named() {
                return name;
            }

            @Override
            public boolean skip(ServerWebExchange exchange) {
                return true;
            }
        };
    }

    private ServerWebExchange createMockExchange(String path) {
        MockServerHttpRequest request = MockServerHttpRequest.get(path)
                .remoteAddress(new InetSocketAddress(8090))
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();

        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        ShenyuContext shenyuContext = mock(ShenyuContext.class);
        exchange.getAttributes().put(Constants.CONTEXT, shenyuContext);

        return exchange;
    }

    private ConditionData createConditionData() {
        ConditionData conditionData = new ConditionData();
        conditionData.setParamType(ParamTypeEnum.URI.getName());
        conditionData.setOperator(OperatorEnum.MATCH.getAlias());
        conditionData.setParamValue("/test/**");
        return conditionData;
    }

    @FunctionalInterface
    interface PluginExecutor {
        Mono<Void> execute(ServerWebExchange exchange, ShenyuPluginChain chain);
    }
}
