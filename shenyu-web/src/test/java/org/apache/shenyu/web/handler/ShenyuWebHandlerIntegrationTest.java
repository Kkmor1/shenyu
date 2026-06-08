package org.apache.shenyu.web.handler;

import org.apache.shenyu.common.config.ShenyuConfig;
import org.apache.shenyu.common.dto.ConditionData;
import org.apache.shenyu.common.dto.PluginData;
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.enums.PluginHandlerEventEnum;
import org.apache.shenyu.common.enums.SelectorTypeEnum;
import org.apache.shenyu.plugin.api.ShenyuPlugin;
import org.apache.shenyu.plugin.api.ShenyuPluginChain;
import org.apache.shenyu.plugin.base.cache.BaseDataCache;
import org.apache.shenyu.plugin.base.cache.PluginHandlerEvent;
import org.apache.shenyu.sync.data.api.DiscoveryUpstreamDataSubscriber;
import org.apache.shenyu.sync.data.api.PluginDataSubscriber;
import org.apache.shenyu.web.controller.LocalPluginController;
import org.apache.shenyu.web.loader.ShenyuLoaderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.apache.shenyu.plugin.api.result.ShenyuResult;
import org.apache.shenyu.plugin.api.result.DefaultShenyuResult;
import org.apache.shenyu.plugin.base.alert.AlarmService;
import org.apache.shenyu.web.configuration.SpringExtConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import org.apache.shenyu.web.filter.LocalDispatcherFilter;
import org.springframework.web.reactive.DispatcherHandler;
import org.springframework.web.server.WebFilter;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@SpringBootTest(classes = ShenyuWebHandlerIntegrationTest.TestConfiguration.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class ShenyuWebHandlerIntegrationTest {

    @Autowired
    private WebTestClient webTestClient;

    @Autowired
    private ShenyuWebHandler shenyuWebHandler;

    @Autowired
    private ApplicationEventPublisher eventPublisher;

    @Autowired
    private TestExecutionRecorder executionRecorder;

    @BeforeEach
    public void setup() {
        executionRecorder.clear();
        BaseDataCache.getInstance().cleanPluginData();
        BaseDataCache.getInstance().cleanSelectorData();
        BaseDataCache.getInstance().cleanRuleData();
    }

    @Test
    public void testPluginChainExecutionOrder() {
        // Mock plugins are registered with orders 10, 20, 30
        webTestClient.get().uri("/test")
                .exchange()
                .expectStatus().isOk();

        List<String> executedPlugins = executionRecorder.getExecutedPlugins();
        assertTrue(executedPlugins.contains("plugin10"));
        assertTrue(executedPlugins.contains("plugin20"));
        assertTrue(executedPlugins.contains("plugin30"));
        
        int index10 = executedPlugins.indexOf("plugin10");
        int index20 = executedPlugins.indexOf("plugin20");
        int index30 = executedPlugins.indexOf("plugin30");
        
        assertTrue(index10 < index20);
        assertTrue(index20 < index30);
    }

    @Test
    public void testPluginSkipWhenEmptyMono() {
        // enable the plugin that returns empty mono
        executionRecorder.setEmptyMonoPluginEnabled(true);

        webTestClient.get().uri("/test")
                .exchange()
                .expectStatus().isOk();

        List<String> executedPlugins = executionRecorder.getExecutedPlugins();
        assertTrue(executedPlugins.contains("plugin10"));
        assertTrue(executedPlugins.contains("pluginEmpty"));
        assertTrue(!executedPlugins.contains("plugin20"));
        assertTrue(!executedPlugins.contains("plugin30"));

        executionRecorder.setEmptyMonoPluginEnabled(false);
    }

    @Test
    public void testPluginHotReloading() {
        // Initially disable plugin40
        PluginData disableData = PluginData.builder()
                .name("plugin40")
                .enabled(false)
                .build();
        eventPublisher.publishEvent(new PluginHandlerEvent(PluginHandlerEventEnum.DISABLED, disableData));

        executionRecorder.clear();
        webTestClient.get().uri("/test")
                .exchange()
                .expectStatus().isOk();
        List<String> executedPlugins = executionRecorder.getExecutedPlugins();
        assertTrue(!executedPlugins.contains("plugin40"));

        executionRecorder.clear();

        // Hot reload (enable) plugin40
        PluginData enableData = PluginData.builder()
                .name("plugin40")
                .enabled(true)
                .build();
        eventPublisher.publishEvent(new PluginHandlerEvent(PluginHandlerEventEnum.ENABLED, enableData));

        webTestClient.get().uri("/test")
                .exchange()
                .expectStatus().isOk();
        executedPlugins = executionRecorder.getExecutedPlugins();
        assertTrue(executedPlugins.contains("plugin40"));
    }

    @Test
    public void testGlobalExceptionHandler() {
        executionRecorder.setThrowException(true);

        webTestClient.get().uri("/test")
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);

        executionRecorder.setThrowException(false);
    }

    @Test
    public void testLocalPluginControllerRuleUpdate() {
        // Simulate LocalPluginController behavior updating rules
        RuleData ruleData = new RuleData();
        ruleData.setPluginName("plugin10");
        ruleData.setName("testRule");
        ruleData.setSelectorId("testSelectorId");
        
        webTestClient.post().uri("/shenyu/plugin/rule/saveOrUpdate")
                .header("localKey", "test-key")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(ruleData)
                .exchange()
                .expectStatus().isOk();

        List<RuleData> cachedRules = BaseDataCache.getInstance().obtainRuleData("testSelectorId");
        assertEquals(1, cachedRules.size());
        assertEquals("testRule", cachedRules.get(0).getName());

        // Execute plugin chain to ensure it still works after rule update
        webTestClient.get().uri("/test")
                .exchange()
                .expectStatus().isOk();
        List<String> executedPlugins = executionRecorder.getExecutedPlugins();
        assertTrue(executedPlugins.contains("plugin10"));
    }

    @Configuration
    @EnableAutoConfiguration
    @Import(SpringExtConfiguration.class)
    static class TestConfiguration {

        @Bean
        public ShenyuResult shenyuResult() {
            return new DefaultShenyuResult();
        }

        @Bean
        public AlarmService alarmService() {
            return content -> { };
        }

        @Bean
        public TestExecutionRecorder testExecutionRecorder() {
            return new TestExecutionRecorder();
        }

        @Bean("dispatcherHandler")
        public DispatcherHandler dispatcherHandler() {
            return new DispatcherHandler();
        }

        @Bean
        public WebFilter localDispatcherFilter(DispatcherHandler dispatcherHandler) {
            // We use an empty sha512Key to disable the key check for testing,
            // or just use the correct hash. Let's use a dummy key and hash it.
            return new LocalDispatcherFilter(dispatcherHandler, org.apache.shenyu.common.utils.DigestUtils.sha512Hex("test-key"));
        }
        @Bean
        public ShenyuConfig shenyuConfig() {
            return new ShenyuConfig();
        }

        @Bean("webHandler")
        public ShenyuWebHandler shenyuWebHandler(List<ShenyuPlugin> plugins, TestExecutionRecorder recorder, ShenyuConfig config) {
            ShenyuLoaderService loaderService = mock(ShenyuLoaderService.class);
            ShenyuConfig.Scheduler scheduler = new ShenyuConfig.Scheduler();
            scheduler.setEnabled(false);
            config.setScheduler(scheduler);
            return new ShenyuWebHandler(plugins, loaderService, config);
        }

        @Bean
        public ShenyuPlugin plugin10(TestExecutionRecorder recorder) {
            return new MockShenyuPlugin("plugin10", 10, recorder);
        }

        @Bean
        public ShenyuPlugin plugin20(TestExecutionRecorder recorder) {
            return new MockShenyuPlugin("plugin20", 20, recorder);
        }

        @Bean
        public ShenyuPlugin plugin30(TestExecutionRecorder recorder) {
            return new MockShenyuPlugin("plugin30", 30, recorder);
        }

        @Bean
        public ShenyuPlugin pluginEmpty(TestExecutionRecorder recorder) {
            return new ShenyuPlugin() {
                @Override
                public Mono<Void> execute(ServerWebExchange exchange, ShenyuPluginChain chain) {
                    if (recorder.isEmptyMonoPluginEnabled()) {
                        recorder.record("pluginEmpty");
                        return Mono.empty();
                    }
                    return chain.execute(exchange);
                }
                @Override
                public int getOrder() { return 15; }
                @Override
                public String named() { return "pluginEmpty"; }
            };
        }

        @Bean
        public ShenyuPlugin plugin40(TestExecutionRecorder recorder) {
            return new ShenyuPlugin() {
                @Override
                public Mono<Void> execute(ServerWebExchange exchange, ShenyuPluginChain chain) {
                    recorder.record("plugin40");
                    return chain.execute(exchange);
                }
                @Override
                public int getOrder() { return 40; }
                @Override
                public String named() { return "plugin40"; }
            };
        }

        @Bean
        public ShenyuPlugin pluginException(TestExecutionRecorder recorder) {
            return new ShenyuPlugin() {
                @Override
                public Mono<Void> execute(ServerWebExchange exchange, ShenyuPluginChain chain) {
                    if (recorder.isThrowException()) {
                        throw new RuntimeException("Test Exception");
                    }
                    return chain.execute(exchange);
                }
                @Override
                public int getOrder() { return 5; }
                @Override
                public String named() { return "pluginException"; }
            };
        }

        @Bean
        public LocalPluginController localPluginController() {
            PluginDataSubscriber subscriber = new PluginDataSubscriber() {
                @Override
                public void onSubscribe(PluginData pluginData) {
                    BaseDataCache.getInstance().cachePluginData(pluginData);
                }

                @Override
                public void unSubscribe(PluginData pluginData) {
                    BaseDataCache.getInstance().removePluginData(pluginData);
                }

                @Override
                public void refreshPluginDataAll() {
                    BaseDataCache.getInstance().cleanPluginData();
                }

                @Override
                public void onSelectorSubscribe(SelectorData selectorData) {
                    BaseDataCache.getInstance().cacheSelectData(selectorData);
                }

                @Override
                public void unSelectorSubscribe(SelectorData selectorData) {
                    BaseDataCache.getInstance().removeSelectData(selectorData);
                }

                @Override
                public void refreshSelectorDataAll() {
                    BaseDataCache.getInstance().cleanSelectorData();
                }

                @Override
                public void onRuleSubscribe(RuleData ruleData) {
                    BaseDataCache.getInstance().cacheRuleData(ruleData);
                }

                @Override
                public void unRuleSubscribe(RuleData ruleData) {
                    BaseDataCache.getInstance().removeRuleData(ruleData);
                }

                @Override
                public void refreshRuleDataAll() {
                    BaseDataCache.getInstance().cleanRuleData();
                }
            };
            DiscoveryUpstreamDataSubscriber discoverySubscriber = mock(DiscoveryUpstreamDataSubscriber.class);
            return new LocalPluginController(subscriber, discoverySubscriber);
        }

        @Bean
        public GlobalErrorHandler globalErrorHandler() {
            return new GlobalErrorHandler();
        }
        
        @RestController
        static class TestController {
            @GetMapping("/test")
            public Mono<String> test() {
                return Mono.just("test");
            }
        }
    }

    static class MockShenyuPlugin implements ShenyuPlugin {
        private final String name;
        private final int order;
        private final TestExecutionRecorder recorder;

        public MockShenyuPlugin(String name, int order, TestExecutionRecorder recorder) {
            this.name = name;
            this.order = order;
            this.recorder = recorder;
        }

        @Override
        public Mono<Void> execute(ServerWebExchange exchange, ShenyuPluginChain chain) {
            recorder.record(name);
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

    static class TestExecutionRecorder {
        private final List<String> executedPlugins = new CopyOnWriteArrayList<>();
        private boolean emptyMonoPluginEnabled = false;
        private boolean throwException = false;

        public void record(String pluginName) {
            executedPlugins.add(pluginName);
        }

        public List<String> getExecutedPlugins() {
            return executedPlugins;
        }

        public void clear() {
            executedPlugins.clear();
        }

        public boolean isEmptyMonoPluginEnabled() {
            return emptyMonoPluginEnabled;
        }

        public void setEmptyMonoPluginEnabled(boolean emptyMonoPluginEnabled) {
            this.emptyMonoPluginEnabled = emptyMonoPluginEnabled;
        }

        public boolean isThrowException() {
            return throwException;
        }

        public void setThrowException(boolean throwException) {
            this.throwException = throwException;
        }
    }
}
