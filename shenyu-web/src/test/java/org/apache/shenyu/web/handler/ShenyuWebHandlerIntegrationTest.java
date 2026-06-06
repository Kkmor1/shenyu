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
import org.apache.shenyu.common.dto.PluginData;
import org.apache.shenyu.common.enums.PluginHandlerEventEnum;
import org.apache.shenyu.plugin.api.ShenyuPlugin;
import org.apache.shenyu.plugin.api.ShenyuPluginChain;
import org.apache.shenyu.plugin.api.context.ShenyuContext;
import org.apache.shenyu.plugin.base.cache.BaseDataCache;
import org.apache.shenyu.plugin.base.cache.PluginHandlerEvent;
import org.apache.shenyu.web.loader.ShenyuLoaderService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Integration test for {@link ShenyuWebHandler}.
 */
@ExtendWith(MockitoExtension.class)
public class ShenyuWebHandlerIntegrationTest {

    private ShenyuWebHandler shenyuWebHandler;

    @Mock
    private ShenyuLoaderService shenyuLoaderService;

    private List<ShenyuPlugin> testPlugins;

    private List<String> executionOrder;

    @BeforeEach
    public void setUp() {
        executionOrder = new ArrayList<>();
        testPlugins = createTestPlugins();
        shenyuWebHandler = new ShenyuWebHandler(testPlugins, shenyuLoaderService, new ShenyuConfig());
        BaseDataCache.getInstance().cleanAll();
    }

    @AfterEach
    public void tearDown() {
        executionOrder.clear();
        BaseDataCache.getInstance().cleanAll();
    }

    @Test
    public void testPluginChainExecuteInCorrectOrder() {
        // Create test plugins with different orders
        TrackingPlugin plugin1 = new TrackingPlugin("plugin1", 1, executionOrder);
        TrackingPlugin plugin2 = new TrackingPlugin("plugin2", 2, executionOrder);
        TrackingPlugin plugin3 = new TrackingPlugin("plugin3", 3, executionOrder);

        List<ShenyuPlugin> plugins = Arrays.asList(plugin3, plugin1, plugin2);
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, new ShenyuConfig());

        ServerWebExchange exchange = createTestExchange();
        StepVerifier.create(handler.handle(exchange))
                .expectSubscription()
                .verifyComplete();

        // Verify execution order based on order value
        assertEquals(Arrays.asList("plugin1", "plugin2", "plugin3"), executionOrder);
    }

    @Test
    public void testPluginReturnMonoEmptySkipsSubsequentPlugins() {
        // Create plugins where the second plugin returns empty
        TrackingPlugin plugin1 = new TrackingPlugin("plugin1", 1, executionOrder);
        EmptyReturnPlugin plugin2 = new EmptyReturnPlugin("plugin2", 2, executionOrder);
        TrackingPlugin plugin3 = new TrackingPlugin("plugin3", 3, executionOrder);

        List<ShenyuPlugin> plugins = Arrays.asList(plugin1, plugin2, plugin3);
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, new ShenyuConfig());

        ServerWebExchange exchange = createTestExchange();
        StepVerifier.create(handler.handle(exchange))
                .expectSubscription()
                .verifyComplete();

        // Verify only plugin1 and plugin2 executed
        assertEquals(Arrays.asList("plugin1", "plugin2"), executionOrder);
    }

    @Test
    public void testPluginHotLoadTakesEffectImmediately() {
        // Setup initial plugins
        TrackingPlugin plugin1 = new TrackingPlugin("plugin1", 1, executionOrder);
        testPlugins = Arrays.asList(plugin1);
        shenyuWebHandler = new ShenyuWebHandler(testPlugins, shenyuLoaderService, new ShenyuConfig());

        // First request - only plugin1 should execute
        ServerWebExchange exchange1 = createTestExchange();
        StepVerifier.create(shenyuWebHandler.handle(exchange1))
                .expectSubscription()
                .verifyComplete();
        assertEquals(Collections.singletonList("plugin1"), executionOrder);

        // Hot load new plugin
        TrackingPlugin plugin2 = new TrackingPlugin("plugin2", 2, executionOrder);
        PluginData pluginData = PluginData.builder()
                .id("2")
                .name("plugin2")
                .enabled(true)
                .sort(2)
                .build();

        // Update source plugins to include the new plugin
        shenyuWebHandler.putExtPlugins(Collections.singletonList(plugin2));
        
        // Trigger plugin enable event
        shenyuWebHandler.onApplicationEvent(new PluginHandlerEvent(PluginHandlerEventEnum.ENABLED, pluginData));

        // Second request - both plugins should execute
        executionOrder.clear();
        ServerWebExchange exchange2 = createTestExchange();
        StepVerifier.create(shenyuWebHandler.handle(exchange2))
                .expectSubscription()
                .verifyComplete();
        
        assertEquals(Arrays.asList("plugin1", "plugin2"), executionOrder);
    }

    @Test
    public void testPluginExecutionExceptionHandling() {
        // Create a plugin that throws exception
        ExceptionThrowingPlugin exceptionPlugin = new ExceptionThrowingPlugin("exceptionPlugin", 1);
        TrackingPlugin plugin2 = new TrackingPlugin("plugin2", 2, executionOrder);

        List<ShenyuPlugin> plugins = Arrays.asList(exceptionPlugin, plugin2);
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, new ShenyuConfig());

        ServerWebExchange exchange = createTestExchange();
        
        // Execute and verify exception propagates
        StepVerifier.create(handler.handle(exchange))
                .expectError(RuntimeException.class)
                .verify();
        
        // Verify plugin2 didn't execute
        assertTrue(executionOrder.isEmpty());
    }

    @Test
    public void testLocalPluginControllerRuleUpdateAffectsPluginChain() {
        // Setup initial plugins with sort from BaseDataCache
        TrackingPlugin plugin1 = new TrackingPlugin("plugin1", 1, executionOrder);
        TrackingPlugin plugin2 = new TrackingPlugin("plugin2", 2, executionOrder);

        // Cache initial plugin data with specific sort values
        PluginData pluginData1 = PluginData.builder()
                .id("1")
                .name("plugin1")
                .enabled(true)
                .sort(20) // Higher sort value means later execution
                .build();
        
        PluginData pluginData2 = PluginData.builder()
                .id("2")
                .name("plugin2")
                .enabled(true)
                .sort(10) // Lower sort value means earlier execution
                .build();
        
        BaseDataCache.getInstance().cachePluginData(pluginData1);
        BaseDataCache.getInstance().cachePluginData(pluginData2);

        List<ShenyuPlugin> plugins = Arrays.asList(plugin1, plugin2);
        ShenyuWebHandler handler = new ShenyuWebHandler(plugins, shenyuLoaderService, new ShenyuConfig());

        // Trigger sorted event to reorder plugins based on BaseDataCache
        handler.onApplicationEvent(new PluginHandlerEvent(PluginHandlerEventEnum.SORTED, pluginData1));

        // First request - plugin2 should execute first due to sort=10
        ServerWebExchange exchange1 = createTestExchange();
        StepVerifier.create(handler.handle(exchange1))
                .expectSubscription()
                .verifyComplete();
        assertEquals(Arrays.asList("plugin2", "plugin1"), executionOrder);

        // Update plugin sort values (simulating LocalPluginController update)
        pluginData1.setSort(5);  // plugin1 now has lower sort
        pluginData2.setSort(15); // plugin2 now has higher sort
        BaseDataCache.getInstance().cachePluginData(pluginData1);
        BaseDataCache.getInstance().cachePluginData(pluginData2);

        // Trigger sorted event again
        handler.onApplicationEvent(new PluginHandlerEvent(PluginHandlerEventEnum.SORTED, pluginData1));

        // Second request - plugin1 should execute first now
        executionOrder.clear();
        ServerWebExchange exchange2 = createTestExchange();
        StepVerifier.create(handler.handle(exchange2))
                .expectSubscription()
                .verifyComplete();
        assertEquals(Arrays.asList("plugin1", "plugin2"), executionOrder);
    }

    private ServerWebExchange createTestExchange() {
        MockServerHttpRequest request = MockServerHttpRequest.get("localhost")
                .remoteAddress(new InetSocketAddress(8090))
                .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);
        exchange.getAttributes().put(Constants.CONTEXT, mock(ShenyuContext.class));
        return exchange;
    }

    private List<ShenyuPlugin> createTestPlugins() {
        return new ArrayList<>();
    }

    /**
     * Test plugin that tracks execution order.
     */
    static class TrackingPlugin implements ShenyuPlugin {
        private final String name;
        private final int order;
        private final List<String> executionOrder;

        TrackingPlugin(final String name, final int order, final List<String> executionOrder) {
            this.name = name;
            this.order = order;
            this.executionOrder = executionOrder;
        }

        @Override
        public Mono<Void> execute(final ServerWebExchange exchange, final ShenyuPluginChain chain) {
            executionOrder.add(name);
            return chain.execute(exchange);
        }

        @Override
        public int getOrder() {
            return order;
        }

        @Override
        public String named() {
            return name;
        }
    }

    /**
     * Test plugin that returns Mono.empty().
     */
    static class EmptyReturnPlugin implements ShenyuPlugin {
        private final String name;
        private final int order;
        private final List<String> executionOrder;

        EmptyReturnPlugin(final String name, final int order, final List<String> executionOrder) {
            this.name = name;
            this.order = order;
            this.executionOrder = executionOrder;
        }

        @Override
        public Mono<Void> execute(final ServerWebExchange exchange, final ShenyuPluginChain chain) {
            executionOrder.add(name);
            return Mono.empty();
        }

        @Override
        public int getOrder() {
            return order;
        }

        @Override
        public String named() {
            return name;
        }
    }

    /**
     * Test plugin that throws exception.
     */
    static class ExceptionThrowingPlugin implements ShenyuPlugin {
        private final String name;
        private final int order;

        ExceptionThrowingPlugin(final String name, final int order) {
            this.name = name;
            this.order = order;
        }

        @Override
        public Mono<Void> execute(final ServerWebExchange exchange, final ShenyuPluginChain chain) {
            return Mono.error(new RuntimeException("Test exception"));
        }

        @Override
        public int getOrder() {
            return order;
        }

        @Override
        public String named() {
            return name;
        }
    }
}
