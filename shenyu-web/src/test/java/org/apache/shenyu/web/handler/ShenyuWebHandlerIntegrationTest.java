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
import org.apache.shenyu.common.dto.ConditionData;
import org.apache.shenyu.common.dto.PluginData;
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.enums.MatchModeEnum;
import org.apache.shenyu.common.enums.OperatorEnum;
import org.apache.shenyu.common.enums.ParamTypeEnum;
import org.apache.shenyu.common.enums.PluginHandlerEventEnum;
import org.apache.shenyu.plugin.api.ShenyuPlugin;
import org.apache.shenyu.plugin.api.ShenyuPluginChain;
import org.apache.shenyu.plugin.api.result.DefaultShenyuResult;
import org.apache.shenyu.plugin.api.result.ShenyuResult;
import org.apache.shenyu.plugin.api.utils.SpringBeanUtils;
import org.apache.shenyu.plugin.base.alert.AlarmService;
import org.apache.shenyu.plugin.base.cache.BaseDataCache;
import org.apache.shenyu.plugin.base.cache.CommonPluginDataSubscriber;
import org.apache.shenyu.plugin.base.cache.PluginHandlerEvent;
import org.apache.shenyu.plugin.base.handler.PluginDataHandler;
import org.apache.shenyu.sync.data.api.PluginDataSubscriber;
import org.apache.shenyu.web.configuration.ErrorHandlerConfiguration;
import org.apache.shenyu.web.configuration.ShenyuExtConfiguration;
import org.apache.shenyu.web.controller.LocalPluginController;
import org.apache.shenyu.web.loader.ShenyuLoaderService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for ShenyuWebHandler.
 * This test covers 5 core scenarios:
 * 1. Plugin chain execution in correct priority order
 * 2. Skip subsequent plugins when plugin returns Mono.empty()
 * 3. New plugin chain takes effect immediately after hot reload
 * 4. Global exception handling when plugin throws exception
 * 5. Plugin chain execution after updating rules through LocalPluginController in local mode
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        classes = ShenyuWebHandlerIntegrationTest.TestConfig.class
)
public class ShenyuWebHandlerIntegrationTest {

    private WebTestClient webTestClient;

    @Autowired
    private ShenyuWebHandler shenyuWebHandler;

    @Autowired
    private ApplicationEventPublisher eventPublisher;

    @Autowired
    private TestExecutionContext executionContext;

    @BeforeEach
    void setUp() {
        executionContext.clear();
        BaseDataCache.getInstance().cleanPluginData();
        BaseDataCache.getInstance().cleanSelectorData();
        BaseDataCache.getInstance().cleanRuleData();
        this.webTestClient = WebTestClient.bindToWebHandler(shenyuWebHandler).build();
    }

    @AfterEach
    void tearDown() {
        executionContext.clear();
        BaseDataCache.getInstance().cleanPluginData();
        BaseDataCache.getInstance().cleanSelectorData();
        BaseDataCache.getInstance().cleanRuleData();
    }

    @TestConfiguration
    @Import({ErrorHandlerConfiguration.class, ShenyuExtConfiguration.class})
    static class TestConfig {

        @Bean
        public TestExecutionContext executionContext() {
            return new TestExecutionContext();
        }

        @Bean
        public PriorityPlugin plugin1(TestExecutionContext context) {
            return new PriorityPlugin(1, "plugin1", context);
        }

        @Bean
        public PriorityPlugin plugin2(TestExecutionContext context) {
            return new PriorityPlugin(2, "plugin2", context);
        }

        @Bean
        public PriorityPlugin plugin3(TestExecutionContext context) {
            return new PriorityPlugin(3, "plugin3", context);
        }

        @Bean
        public ResponseWritingPlugin responsePlugin() {
            return new ResponseWritingPlugin(999, "response-plugin");
        }

        @Bean
        public ShenyuLoaderService shenyuLoaderService(ShenyuWebHandler shenyuWebHandler,
                                                       PluginDataSubscriber pluginDataSubscriber,
                                                       ShenyuConfig shenyuConfig) {
            return new ShenyuLoaderService(shenyuWebHandler, (CommonPluginDataSubscriber) pluginDataSubscriber, shenyuConfig);
        }

        @Bean
        public ShenyuWebHandler shenyuWebHandler(List<ShenyuPlugin> plugins,
                                                  ShenyuLoaderService shenyuLoaderService,
                                                  ShenyuConfig shenyuConfig) {
            return new ShenyuWebHandler(plugins, shenyuLoaderService, shenyuConfig);
        }

        @Bean
        public ShenyuConfig shenyuConfig() {
            return new ShenyuConfig();
        }

        @Bean
        public PluginDataSubscriber pluginDataSubscriber(ApplicationEventPublisher eventPublisher,
                                                          ShenyuConfig shenyuConfig) {
            return new CommonPluginDataSubscriber(Collections.<PluginDataHandler>emptyList(), eventPublisher,
                    shenyuConfig.getSelectorMatchCache(), shenyuConfig.getRuleMatchCache());
        }

        @Bean
        public LocalPluginController localPluginController(PluginDataSubscriber pluginDataSubscriber) {
            return new LocalPluginController(pluginDataSubscriber, null);
        }

        @Bean
        public ConfigurableApplicationContext fixer(ConfigurableApplicationContext context) {
            SpringBeanUtils.getInstance().setApplicationContext(context);
            return context;
        }

        @Bean
        public AlarmService alarmService() {
            return content -> {
            };
        }

        @Bean
        public ShenyuResult<?> shenyuResult() {
            return new DefaultShenyuResult();
        }
    }

    static class TestExecutionContext {
        private final CopyOnWriteArrayList<String> executionOrder = new CopyOnWriteArrayList<>();

        public void recordExecution(String pluginName) {
            executionOrder.add(pluginName);
        }

        public void clear() {
            executionOrder.clear();
        }

        public List<String> getExecutionOrder() {
            return new ArrayList<>(executionOrder);
        }

        public int size() {
            return executionOrder.size();
        }
    }

    static class PriorityPlugin implements ShenyuPlugin {
        private final int order;
        private final String name;
        private final TestExecutionContext context;

        PriorityPlugin(int order, String name, TestExecutionContext context) {
            this.order = order;
            this.name = name;
            this.context = context;
        }

        @Override
        public Mono<Void> execute(ServerWebExchange exchange, ShenyuPluginChain chain) {
            context.recordExecution(name);
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

    static class EmptyReturnPlugin implements ShenyuPlugin {
        private final int order;
        private final String name;
        private final TestExecutionContext context;

        EmptyReturnPlugin(int order, String name, TestExecutionContext context) {
            this.order = order;
            this.name = name;
            this.context = context;
        }

        @Override
        public Mono<Void> execute(ServerWebExchange exchange, ShenyuPluginChain chain) {
            context.recordExecution(name);
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

    static class ExceptionThrowingPlugin implements ShenyuPlugin {
        private final int order;
        private final String name;
        private final TestExecutionContext context;
        private final RuntimeException exception;

        ExceptionThrowingPlugin(int order, String name, TestExecutionContext context, RuntimeException exception) {
            this.order = order;
            this.name = name;
            this.context = context;
            this.exception = exception;
        }

        @Override
        public Mono<Void> execute(ServerWebExchange exchange, ShenyuPluginChain chain) {
            context.recordExecution(name);
            throw exception;
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

    static class RecordingPlugin implements ShenyuPlugin {
        private final int order;
        private final String name;
        private final TestExecutionContext context;

        RecordingPlugin(int order, String name, TestExecutionContext context) {
            this.order = order;
            this.name = name;
            this.context = context;
        }

        @Override
        public Mono<Void> execute(ServerWebExchange exchange, ShenyuPluginChain chain) {
            context.recordExecution(name);
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

    static class ResponseWritingPlugin implements ShenyuPlugin {
        private final int order;
        private final String name;

        ResponseWritingPlugin(int order, String name) {
            this.order = order;
            this.name = name;
        }

        @Override
        public Mono<Void> execute(ServerWebExchange exchange, ShenyuPluginChain chain) {
            exchange.getResponse().setStatusCode(HttpStatus.OK);
            byte[] bytes = "ok".getBytes(StandardCharsets.UTF_8);
            DataBuffer buffer = new DefaultDataBufferFactory().wrap(bytes);
            return exchange.getResponse().writeWith(Mono.just(buffer));
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

    @Test
    void testPluginChainExecutedInCorrectPriorityOrder() {
        webTestClient.get().uri("/test/plugin/chain")
                .accept(MediaType.ALL)
                .exchange()
                .expectStatus().isOk();

        assertEquals(4, executionContext.size());
        assertEquals("plugin1", executionContext.getExecutionOrder().get(0));
        assertEquals("plugin2", executionContext.getExecutionOrder().get(1));
        assertEquals("plugin3", executionContext.getExecutionOrder().get(2));
        assertEquals("response-plugin", executionContext.getExecutionOrder().get(3));
    }

    @Test
    void testSkipSubsequentPluginsWhenMonoEmpty() {
        shenyuWebHandler.putExtPlugins(List.of(new EmptyReturnPlugin(2, "empty-plugin", executionContext)));

        webTestClient.get().uri("/test/plugin/empty")
                .accept(MediaType.ALL)
                .exchange()
                .expectStatus().isOk();

        List<String> executionOrder = executionContext.getExecutionOrder();
        assertEquals(2, executionOrder.size());
        assertEquals("plugin1", executionOrder.get(0));
        assertEquals("empty-plugin", executionOrder.get(1));
        assertFalse(executionOrder.contains("plugin3"), "plugin3 should be skipped");
        assertFalse(executionOrder.contains("response-plugin"), "response-plugin should be skipped");
    }

    @Test
    void testHotReloadNewPluginChainTakesEffectImmediately() {
        webTestClient.get().uri("/test/plugin/chain")
                .accept(MediaType.ALL)
                .exchange()
                .expectStatus().isOk();
        assertEquals(4, executionContext.size());

        executionContext.clear();

        PluginData newPluginData = PluginData.builder()
                .name("new-plugin")
                .enabled(true)
                .sort(0)
                .build();
        BaseDataCache.getInstance().cachePluginData(newPluginData);
        eventPublisher.publishEvent(new PluginHandlerEvent(PluginHandlerEventEnum.ENABLED, newPluginData));

        shenyuWebHandler.putExtPlugins(List.of(new RecordingPlugin(0, "new-plugin", executionContext)));

        webTestClient.get().uri("/test/plugin/chain")
                .accept(MediaType.ALL)
                .exchange()
                .expectStatus().isOk();

        List<String> executionOrder = executionContext.getExecutionOrder();
        assertEquals(5, executionOrder.size());
        assertEquals("new-plugin", executionOrder.get(0), "New plugin should be first after hot reload");
        assertEquals("plugin1", executionOrder.get(1));
        assertEquals("plugin2", executionOrder.get(2));
        assertEquals("plugin3", executionOrder.get(3));
        assertEquals("response-plugin", executionOrder.get(4));
    }

    @Test
    void testGlobalExceptionHandlingWhenPluginThrowsException() {
        RuntimeException testException = new RuntimeException("Test plugin exception");
        shenyuWebHandler.putExtPlugins(List.of(
                new ExceptionThrowingPlugin(2, "exception-plugin", executionContext, testException)));

        webTestClient.get().uri("/test/plugin/exception")
                .accept(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);

        assertEquals(2, executionContext.size());
        assertEquals("plugin1", executionContext.getExecutionOrder().get(0));
        assertEquals("exception-plugin", executionContext.getExecutionOrder().get(1));
        assertFalse(executionContext.getExecutionOrder().contains("plugin3"),
                "plugin3 should not be executed after exception");
        assertFalse(executionContext.getExecutionOrder().contains("response-plugin"),
                "response-plugin should not be executed after exception");
    }

    @Test
    void testLocalModeUpdateRuleThroughLocalPluginController() {
        String testPluginName = "test-local-plugin";

        PluginData pluginData = PluginData.builder()
                .id("1")
                .name(testPluginName)
                .enabled(true)
                .sort(0)
                .build();
        BaseDataCache.getInstance().cachePluginData(pluginData);
        eventPublisher.publishEvent(new PluginHandlerEvent(PluginHandlerEventEnum.ENABLED, pluginData));

        shenyuWebHandler.putExtPlugins(List.of(new RecordingPlugin(0, testPluginName, executionContext)));

        ConditionData conditionData = new ConditionData();
        conditionData.setParamType(ParamTypeEnum.URI.getName());
        conditionData.setOperator(OperatorEnum.EQ.getAlias());
        conditionData.setParamValue("/test/plugin/local/update");

        SelectorData selectorData = SelectorData.builder()
                .id("selector-1")
                .pluginName(testPluginName)
                .matchMode(MatchModeEnum.AND.getCode())
                .conditionList(Collections.singletonList(conditionData))
                .enabled(true)
                .sort(10)
                .build();
        BaseDataCache.getInstance().cacheSelectData(selectorData);

        RuleData ruleData = RuleData.builder()
                .id("rule-1")
                .selectorId("selector-1")
                .pluginName(testPluginName)
                .conditionDataList(Collections.singletonList(conditionData))
                .enabled(true)
                .handle("{}")
                .build();
        BaseDataCache.getInstance().cacheRuleData(ruleData);

        executionContext.clear();

        webTestClient.get().uri("/test/plugin/local/update")
                .accept(MediaType.ALL)
                .exchange()
                .expectStatus().isOk();

        assertTrue(executionContext.getExecutionOrder().contains(testPluginName),
                "Newly added plugin should be executed after local rule update. Actual: "
                        + executionContext.getExecutionOrder());
        assertTrue(executionContext.getExecutionOrder().indexOf(testPluginName)
                < executionContext.getExecutionOrder().indexOf("plugin1"),
                "Plugin should be executed according to sorted order. Actual: "
                        + executionContext.getExecutionOrder());
    }
}