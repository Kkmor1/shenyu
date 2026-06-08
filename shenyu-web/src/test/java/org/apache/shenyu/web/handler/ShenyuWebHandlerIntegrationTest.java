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
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.utils.DigestUtils;
import org.apache.shenyu.plugin.api.ShenyuPlugin;
import org.apache.shenyu.plugin.api.ShenyuPluginChain;
import org.apache.shenyu.plugin.api.result.DefaultShenyuEntity;
import org.apache.shenyu.plugin.api.result.DefaultShenyuResult;
import org.apache.shenyu.plugin.api.result.ShenyuResult;
import org.apache.shenyu.plugin.api.utils.WebFluxResultUtils;
import org.apache.shenyu.plugin.base.alert.AlarmService;
import org.apache.shenyu.plugin.base.cache.BaseDataCache;
import org.apache.shenyu.plugin.base.cache.CommonDiscoveryUpstreamDataSubscriber;
import org.apache.shenyu.plugin.base.cache.CommonPluginDataSubscriber;
import org.apache.shenyu.plugin.base.handler.DiscoveryUpstreamDataHandler;
import org.apache.shenyu.plugin.base.handler.PluginDataHandler;
import org.apache.shenyu.sync.data.api.DiscoveryUpstreamDataSubscriber;
import org.apache.shenyu.sync.data.api.PluginDataSubscriber;
import org.apache.shenyu.web.configuration.ErrorHandlerConfiguration;
import org.apache.shenyu.web.configuration.SpringExtConfiguration;
import org.apache.shenyu.web.controller.LocalPluginController;
import org.apache.shenyu.web.filter.LocalDispatcherFilter;
import org.apache.shenyu.web.loader.ShenyuLoaderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.DispatcherHandler;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@SpringBootTest(
        classes = ShenyuWebHandlerIntegrationTest.TestApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        properties = {
                "spring.main.web-application-type=reactive",
                "spring.main.allow-bean-definition-overriding=true"
        }
)
@AutoConfigureWebTestClient
public class ShenyuWebHandlerIntegrationTest {

    private static final String TRACE_ATTR = "integrationTrace";

    private static final String LOCAL_KEY_VALUE = "integration-local-key";

    @Autowired
    private WebTestClient webTestClient;

    @Autowired
    private ShenyuWebHandler shenyuWebHandler;

    @BeforeEach
    void setUp() {
        BaseDataCache.getInstance().cleanPluginData();
        BaseDataCache.getInstance().cleanSelectorData();
        BaseDataCache.getInstance().cleanRuleData();
    }

    @Test
    void pluginChainExecutesByPriorityOrder() {
        webTestClient.get()
                .uri("/test/order")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .isEqualTo("plugin-order-10>plugin-order-20>plugin-order-30");
    }

    @Test
    void monoEmptyStopsFollowingPlugins() {
        webTestClient.get()
                .uri("/test/empty")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Trace", "empty-start>empty-stop")
                .expectHeader().doesNotExist("X-After-Empty")
                .expectBody().isEmpty();
    }

    @Test
    void hotLoadedPluginChainTakesEffectImmediately() {
        webTestClient.get()
                .uri("/test/hot-load")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .isEqualTo("hot-base");

        shenyuWebHandler.putExtPlugins(Collections.singletonList(new TracePlugin("hot-dynamic", 150, "/test/hot-load")));

        webTestClient.get()
                .uri("/test/hot-load")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .isEqualTo("hot-dynamic>hot-base");
    }

    @Test
    void pluginExceptionUsesGlobalErrorHandler() {
        webTestClient.get()
                .uri("/test/exception")
                .exchange()
                .expectStatus().is5xxServerError()
                .expectHeader().contentType(MediaType.APPLICATION_JSON)
                .expectBody()
                .jsonPath("$.code").isEqualTo(500)
                .jsonPath("$.message").isEqualTo("Internal Server Error");
    }

    @Test
    void localModeRuleUpdateAffectsPluginChainExecution() {
        SelectorData selectorData = SelectorData.builder()
                .id("local-selector")
                .pluginName("local-rule-plugin")
                .build();
        RuleData initialRule = RuleData.builder()
                .id("local-rule")
                .selectorId("local-selector")
                .pluginName("local-rule-plugin")
                .handle("rule-v1")
                .build();
        RuleData updatedRule = RuleData.builder()
                .id("local-rule")
                .selectorId("local-selector")
                .pluginName("local-rule-plugin")
                .handle("rule-v2")
                .build();

        webTestClient.post()
                .uri("/shenyu/plugin/selector/saveOrUpdate")
                .header(Constants.LOCAL_KEY, LOCAL_KEY_VALUE)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(selectorData)
                .exchange()
                .expectStatus().isOk();

        webTestClient.post()
                .uri("/shenyu/plugin/rule/saveOrUpdate")
                .header(Constants.LOCAL_KEY, LOCAL_KEY_VALUE)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(initialRule)
                .exchange()
                .expectStatus().isOk();

        webTestClient.get()
                .uri("/test/local-rule")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .isEqualTo("rule-v1");

        webTestClient.post()
                .uri("/shenyu/plugin/rule/saveOrUpdate")
                .header(Constants.LOCAL_KEY, LOCAL_KEY_VALUE)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(updatedRule)
                .exchange()
                .expectStatus().isOk();

        webTestClient.get()
                .uri("/test/local-rule")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .isEqualTo("rule-v2");
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({SpringExtConfiguration.class, ErrorHandlerConfiguration.class})
    static class TestApplication {

        @Bean
        public ShenyuConfig shenyuConfig() {
            ShenyuConfig shenyuConfig = new ShenyuConfig();
            shenyuConfig.getLocal().setEnabled(true);
            shenyuConfig.getLocal().setSha512Key(DigestUtils.sha512Hex(LOCAL_KEY_VALUE));
            shenyuConfig.getAlert().setEnabled(false);
            return shenyuConfig;
        }

        @Bean
        public ShenyuResult<DefaultShenyuEntity> shenyuResult() {
            return new DefaultShenyuResult();
        }

        @Bean
        public AlarmService alarmService() {
            return content -> {
            };
        }

        @Bean
        public PluginDataSubscriber pluginDataSubscriber(final ApplicationEventPublisher eventPublisher,
                                                         final ShenyuConfig shenyuConfig,
                                                         final ObjectProvider<List<PluginDataHandler>> pluginDataHandlers) {
            return new CommonPluginDataSubscriber(
                    pluginDataHandlers.getIfAvailable(Collections::emptyList),
                    eventPublisher,
                    shenyuConfig.getSelectorMatchCache(),
                    shenyuConfig.getRuleMatchCache()
            );
        }

        @Bean
        public DiscoveryUpstreamDataSubscriber discoveryUpstreamDataSubscriber(final ObjectProvider<List<DiscoveryUpstreamDataHandler>> handlers) {
            return new CommonDiscoveryUpstreamDataSubscriber(handlers.getIfAvailable(Collections::emptyList));
        }

        @Bean
        public LocalPluginController localPluginController(final PluginDataSubscriber pluginDataSubscriber,
                                                           final DiscoveryUpstreamDataSubscriber discoveryUpstreamDataSubscriber) {
            return new LocalPluginController(pluginDataSubscriber, discoveryUpstreamDataSubscriber);
        }

        @Bean
        public ShenyuLoaderService shenyuLoaderService() {
            return Mockito.mock(ShenyuLoaderService.class);
        }

        @Bean("webHandler")
        public ShenyuWebHandler shenyuWebHandler(final ObjectProvider<List<ShenyuPlugin>> plugins,
                                                 final ShenyuLoaderService shenyuLoaderService,
                                                 final ShenyuConfig shenyuConfig) {
            List<ShenyuPlugin> pluginList = new ArrayList<>(plugins.getIfAvailable(Collections::emptyList));
            List<ShenyuPlugin> shenyuPlugins = pluginList.stream()
                    .sorted(Comparator.comparingInt(ShenyuPlugin::getOrder))
                    .collect(Collectors.toList());
            return new ShenyuWebHandler(shenyuPlugins, shenyuLoaderService, shenyuConfig);
        }

        @Bean
        @Order(-200)
        public WebFilter localDispatcherFilter(final DispatcherHandler dispatcherHandler, final ShenyuConfig shenyuConfig) {
            return new LocalDispatcherFilter(dispatcherHandler, shenyuConfig.getLocal().getSha512Key());
        }

        @Bean
        public ShenyuPlugin pluginOrderThirty() {
            return new TracePlugin("plugin-order-30", 30, "/test/order");
        }

        @Bean
        public ShenyuPlugin pluginOrderTen() {
            return new TracePlugin("plugin-order-10", 10, "/test/order");
        }

        @Bean
        public ShenyuPlugin pluginOrderTwenty() {
            return new TracePlugin("plugin-order-20", 20, "/test/order");
        }

        @Bean
        public ShenyuPlugin emptyStartPlugin() {
            return new TracePlugin("empty-start", 110, "/test/empty");
        }

        @Bean
        public ShenyuPlugin emptyStopPlugin() {
            return new EmptyStopPlugin("empty-stop", 120, "/test/empty");
        }

        @Bean
        public ShenyuPlugin afterEmptyPlugin() {
            return new AfterEmptyPlugin("after-empty", 130, "/test/empty");
        }

        @Bean
        public ShenyuPlugin hotBasePlugin() {
            return new TracePlugin("hot-base", 200, "/test/hot-load");
        }

        @Bean
        public ShenyuPlugin exceptionPlugin() {
            return new ExceptionPlugin("exception-plugin", 300, "/test/exception");
        }

        @Bean
        public ShenyuPlugin localRulePlugin() {
            return new LocalRulePlugin("local-rule-plugin", 400, "/test/local-rule");
        }

        @Bean
        public ShenyuPlugin responsePlugin() {
            return new TraceResponsePlugin("trace-response", 1000, Set.of("/test/order", "/test/hot-load"));
        }
    }

    private static List<String> trace(final ServerWebExchange exchange) {
        List<String> trace = exchange.getAttribute(TRACE_ATTR);
        if (trace == null) {
            trace = new ArrayList<>();
            exchange.getAttributes().put(TRACE_ATTR, trace);
        }
        return trace;
    }

    private static void appendTrace(final ServerWebExchange exchange, final String pluginName) {
        trace(exchange).add(pluginName);
    }

    private static String currentTrace(final ServerWebExchange exchange) {
        return String.join(">", trace(exchange));
    }

    private static class TracePlugin implements ShenyuPlugin {

        private final String name;

        private final int order;

        private final String path;

        TracePlugin(final String name, final int order, final String path) {
            this.name = name;
            this.order = order;
            this.path = path;
        }

        @Override
        public Mono<Void> execute(final ServerWebExchange exchange, final ShenyuPluginChain chain) {
            appendTrace(exchange, name);
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

        @Override
        public boolean skip(final ServerWebExchange exchange) {
            return !path.equals(exchange.getRequest().getPath().value());
        }
    }

    private static final class EmptyStopPlugin extends TracePlugin {

        EmptyStopPlugin(final String name, final int order, final String path) {
            super(name, order, path);
        }

        @Override
        public Mono<Void> execute(final ServerWebExchange exchange, final ShenyuPluginChain chain) {
            appendTrace(exchange, named());
            exchange.getResponse().getHeaders().add("X-Trace", currentTrace(exchange));
            return Mono.empty();
        }
    }

    private static final class AfterEmptyPlugin extends TracePlugin {

        AfterEmptyPlugin(final String name, final int order, final String path) {
            super(name, order, path);
        }

        @Override
        public Mono<Void> execute(final ServerWebExchange exchange, final ShenyuPluginChain chain) {
            exchange.getResponse().getHeaders().add("X-After-Empty", "executed");
            appendTrace(exchange, named());
            return chain.execute(exchange);
        }
    }

    private static final class ExceptionPlugin extends TracePlugin {

        ExceptionPlugin(final String name, final int order, final String path) {
            super(name, order, path);
        }

        @Override
        public Mono<Void> execute(final ServerWebExchange exchange, final ShenyuPluginChain chain) {
            throw new IllegalStateException("plugin execution failed");
        }
    }

    private static final class LocalRulePlugin extends TracePlugin {

        LocalRulePlugin(final String name, final int order, final String path) {
            super(name, order, path);
        }

        @Override
        public Mono<Void> execute(final ServerWebExchange exchange, final ShenyuPluginChain chain) {
            List<SelectorData> selectors = Optional.ofNullable(BaseDataCache.getInstance().obtainSelectorData(named()))
                    .orElse(Collections.emptyList());
            if (selectors.isEmpty()) {
                return WebFluxResultUtils.result(exchange, "no-selector");
            }
            List<RuleData> rules = Optional.ofNullable(BaseDataCache.getInstance().obtainRuleData(selectors.get(0).getId()))
                    .orElse(Collections.emptyList());
            if (rules.isEmpty()) {
                return WebFluxResultUtils.result(exchange, "no-rule");
            }
            return WebFluxResultUtils.result(exchange, rules.get(0).getHandle());
        }
    }

    private static final class TraceResponsePlugin extends TracePlugin {

        private final Set<String> paths;

        TraceResponsePlugin(final String name, final int order, final Set<String> paths) {
            super(name, order, paths.iterator().next());
            this.paths = paths;
        }

        @Override
        public Mono<Void> execute(final ServerWebExchange exchange, final ShenyuPluginChain chain) {
            return WebFluxResultUtils.result(exchange, currentTrace(exchange));
        }

        @Override
        public boolean skip(final ServerWebExchange exchange) {
            return !paths.contains(exchange.getRequest().getPath().value());
        }
    }
}
