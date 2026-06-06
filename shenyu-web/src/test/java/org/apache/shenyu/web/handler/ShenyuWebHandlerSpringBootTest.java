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
import org.apache.shenyu.plugin.base.cache.BaseDataCache;
import org.apache.shenyu.plugin.base.cache.PluginHandlerEvent;
import org.apache.shenyu.web.loader.ShenyuLoaderService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebHandler;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Spring Boot integration test for {@link ShenyuWebHandler} using WebTestClient.
 */
@SpringBootTest(classes = ShenyuWebHandlerSpringBootTest.TestConfiguration.class)
public class ShenyuWebHandlerSpringBootTest {

    @Autowired
    private WebTestClient webTestClient;

    @Autowired
    private ShenyuWebHandler shenyuWebHandler;

    @Autowired
    private ShenyuLoaderService shenyuLoaderService;

    @Autowired
    private ApplicationEventPublisher eventPublisher;

    private static final List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());

    @BeforeEach
    public void setUp() {
        executionOrder.clear();
        BaseDataCache.getInstance().cleanAll();
    }

    @AfterEach
    public void tearDown() {
        executionOrder.clear();
        BaseDataCache.getInstance().cleanAll();
    }

    @Test
    public void testPluginChainExecuteInCorrectOrder() {
        // Execute test request
        webTestClient.get()
                .uri("/test/order")
                .accept(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus()
                .isOk();

        // Verify execution order
        assertEquals(Arrays.asList("plugin1", "plugin2", "plugin3"), executionOrder);
    }

    @Test
    public void testPluginReturnMonoEmptySkipsSubsequentPlugins() {
        // Update plugin to return empty
        TestPlugin.setReturnEmpty(true);
        
        try {
            webTestClient.get()
                    .uri("/test/empty")
                    .accept(MediaType.APPLICATION_JSON)
                    .exchange()
                    .expectStatus()
                    .isOk();

            // Verify only first two plugins executed
            assertEquals(Arrays.asList("plugin1", "plugin2"), executionOrder);
        } finally {
            TestPlugin.setReturnEmpty(false);
        }
    }

    @Test
    public void testPluginHotLoadTakesEffectImmediately() {
        // First request - only initial plugins should execute
        webTestClient.get()
                .uri("/test/hotload")
                .accept(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus()
                .isOk();
        
        assertEquals(Arrays.asList("plugin1", "plugin2", "plugin3"), executionOrder);
        executionOrder.clear();

        // Hot load a new plugin
        TrackingPlugin newPlugin = new TrackingPlugin("newPlugin", 2, executionOrder);
        shenyuWebHandler.putExtPlugins(Collections.singletonList(newPlugin));

        // Enable the new plugin
        PluginData pluginData = PluginData.builder()
                .id("newPlugin")
                .name("newPlugin")
                .enabled(true)
                .sort(2)
                .build();
        eventPublisher.publishEvent(new PluginHandlerEvent(PluginHandlerEventEnum.ENABLED, pluginData));

        // Second request - new plugin should be in the chain
        webTestClient.get()
                .uri("/test/hotload2")
                .accept(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus()
                .isOk();

        // Verify new plugin is in execution order
        assertTrue(executionOrder.contains("newPlugin"));
    }

    @Test
    public void testPluginExecutionExceptionHandling() {
        // Enable exception throwing
        TestPlugin.setThrowException(true);
        
        try {
            webTestClient.get()
                    .uri("/test/exception")
                    .accept(MediaType.APPLICATION_JSON)
                    .exchange()
                    .expectStatus()
                    .is5xxServerError();
        } finally {
            TestPlugin.setThrowException(false);
        }
    }

    @Test
    public void testLocalPluginControllerRuleUpdateAffectsPluginChain() {
        // Initial request - execution order based on plugin order
        webTestClient.get()
                .uri("/test/sort")
                .accept(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus()
                .isOk();
        
        assertEquals(Arrays.asList("plugin1", "plugin2", "plugin3"), executionOrder);
        executionOrder.clear();

        // Update plugin sort values via BaseDataCache (simulating LocalPluginController)
        PluginData pluginData1 = PluginData.builder()
                .id("1")
                .name("plugin1")
                .enabled(true)
                .sort(30) // Higher sort means later execution
                .build();
        
        PluginData pluginData2 = PluginData.builder()
                .id("2")
                .name("plugin2")
                .enabled(true)
                .sort(20)
                .build();
        
        PluginData pluginData3 = PluginData.builder()
                .id("3")
                .name("plugin3")
                .enabled(true)
                .sort(10) // Lower sort means earlier execution
                .build();
        
        BaseDataCache.getInstance().cachePluginData(pluginData1);
        BaseDataCache.getInstance().cachePluginData(pluginData2);
        BaseDataCache.getInstance().cachePluginData(pluginData3);

        // Publish sorted event to trigger reordering
        eventPublisher.publishEvent(new PluginHandlerEvent(PluginHandlerEventEnum.SORTED, pluginData1));

        // Second request - execution order should now be based on sort values
        webTestClient.get()
                .uri("/test/sort2")
                .accept(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus()
                .isOk();

        // Verify new execution order (plugin3 first due to sort=10)
        assertEquals(Arrays.asList("plugin3", "plugin2", "plugin1"), executionOrder);
    }

    /**
     * Test configuration.
     */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    public static class TestConfiguration {

        @Bean
        public ShenyuWebHandler shenyuWebHandler(final List<ShenyuPlugin> plugins,
                                                  final ShenyuLoaderService shenyuLoaderService,
                                                  final ShenyuConfig shenyuConfig) {
            return new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);
        }

        @Bean
        public ShenyuLoaderService shenyuLoaderService() {
            return mock(ShenyuLoaderService.class);
        }

        @Bean
        public ShenyuConfig shenyuConfig() {
            return new ShenyuConfig();
        }

        @Bean
        public ShenyuPlugin plugin1() {
            return new TrackingPlugin("plugin1", 1, executionOrder);
        }

        @Bean
        public ShenyuPlugin plugin2() {
            return new TestPlugin("plugin2", 2, executionOrder);
        }

        @Bean
        public ShenyuPlugin plugin3() {
            return new TrackingPlugin("plugin3", 3, executionOrder);
        }

        @Bean
        public WebHandler webHandler(final ShenyuWebHandler shenyuWebHandler) {
            return new WebHandler() {
                @Override
                public Mono<Void> handle(final ServerWebExchange exchange) {
                    // Add test context
                    exchange.getAttributes().put(Constants.CONTEXT, new Object());
                    // First execute Shenyu plugin chain
                    return shenyuWebHandler.handle(exchange)
                            .then(Mono.fromRunnable(() -> {
                                // Set response
                                exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
                            }));
                }
            };
        }
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
     * Test plugin with configurable behavior.
     */
    static class TestPlugin implements ShenyuPlugin {
        private static volatile boolean returnEmpty = false;
        private static volatile boolean throwException = false;

        private final String name;
        private final int order;
        private final List<String> executionOrder;

        TestPlugin(final String name, final int order, final List<String> executionOrder) {
            this.name = name;
            this.order = order;
            this.executionOrder = executionOrder;
        }

        static void setReturnEmpty(final boolean value) {
            returnEmpty = value;
        }

        static void setThrowException(final boolean value) {
            throwException = value;
        }

        @Override
        public Mono<Void> execute(final ServerWebExchange exchange, final ShenyuPluginChain chain) {
            executionOrder.add(name);
            
            if (throwException) {
                return Mono.error(new RuntimeException("Test plugin exception"));
            }
            
            if (returnEmpty) {
                return Mono.empty();
            }
            
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
}
