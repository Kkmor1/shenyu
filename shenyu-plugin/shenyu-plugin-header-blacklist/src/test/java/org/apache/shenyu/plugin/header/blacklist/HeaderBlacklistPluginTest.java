package org.apache.shenyu.plugin.header.blacklist;

import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.plugin.api.ShenyuPluginChain;
import org.apache.shenyu.plugin.base.utils.CacheKeyUtils;
import org.apache.shenyu.plugin.header.blacklist.dto.HeaderBlacklistRuleHandle;
import org.apache.shenyu.plugin.header.blacklist.handler.HeaderBlacklistPluginDataHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The type Header blacklist plugin test.
 */
@ExtendWith(MockitoExtension.class)
public class HeaderBlacklistPluginTest {

    @Mock
    private ShenyuPluginChain chain;

    private HeaderBlacklistPlugin headerBlacklistPlugin;

    private RuleData ruleData;

    @BeforeEach
    public void setup() {
        this.headerBlacklistPlugin = new HeaderBlacklistPlugin();
        this.ruleData = new RuleData();
        this.ruleData.setSelectorId("test-selectorId");
        this.ruleData.setName("test-header-blacklist-plugin");
    }

    @Test
    public void testWhitelistMatch() {
        ServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest
                .get("localhost")
                .header("X-Test-Header", "allowed-value")
                .build());

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        
        HeaderBlacklistRuleHandle.MatchRule whiteRule = new HeaderBlacklistRuleHandle.MatchRule();
        whiteRule.setHeader("X-Test-Header");
        whiteRule.setRegex("^allowed-.*$");
        List<HeaderBlacklistRuleHandle.MatchRule> whitelist = new ArrayList<>();
        whitelist.add(whiteRule);
        handle.setWhitelist(whitelist);

        HeaderBlacklistRuleHandle.MatchRule blackRule = new HeaderBlacklistRuleHandle.MatchRule();
        blackRule.setHeader("X-Test-Header");
        blackRule.setRegex(".*");
        List<HeaderBlacklistRuleHandle.MatchRule> blacklist = new ArrayList<>();
        blacklist.add(blackRule);
        handle.setBlacklist(blacklist);

        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(this.ruleData), handle);

        SelectorData selectorData = mock(SelectorData.class);
        when(this.chain.execute(any())).thenReturn(Mono.empty());

        StepVerifier.create(headerBlacklistPlugin.doExecute(exchange, this.chain, selectorData, this.ruleData))
                .expectSubscription()
                .verifyComplete();
    }

    @Test
    public void testBlacklistMatch() {
        ServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest
                .get("localhost")
                .header("X-Test-Header", "forbidden-value")
                .build());

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        
        HeaderBlacklistRuleHandle.MatchRule blackRule = new HeaderBlacklistRuleHandle.MatchRule();
        blackRule.setHeader("X-Test-Header");
        blackRule.setRegex("^forbidden-.*$");
        List<HeaderBlacklistRuleHandle.MatchRule> blacklist = new ArrayList<>();
        blacklist.add(blackRule);
        handle.setBlacklist(blacklist);

        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(this.ruleData), handle);

        SelectorData selectorData = mock(SelectorData.class);

        StepVerifier.create(headerBlacklistPlugin.doExecute(exchange, this.chain, selectorData, this.ruleData))
                .expectSubscription()
                .verifyComplete();

        assertEquals(HttpStatus.FORBIDDEN, exchange.getResponse().getStatusCode());
    }

    @Test
    public void testDefaultStrategy() {
        ServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest
                .get("localhost")
                .header("X-Test-Header", "other-value")
                .build());

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        
        HeaderBlacklistRuleHandle.MatchRule whiteRule = new HeaderBlacklistRuleHandle.MatchRule();
        whiteRule.setHeader("X-Test-Header");
        whiteRule.setRegex("^allowed-.*$");
        List<HeaderBlacklistRuleHandle.MatchRule> whitelist = new ArrayList<>();
        whitelist.add(whiteRule);
        handle.setWhitelist(whitelist);

        HeaderBlacklistRuleHandle.MatchRule blackRule = new HeaderBlacklistRuleHandle.MatchRule();
        blackRule.setHeader("X-Test-Header");
        blackRule.setRegex("^forbidden-.*$");
        List<HeaderBlacklistRuleHandle.MatchRule> blacklist = new ArrayList<>();
        blacklist.add(blackRule);
        handle.setBlacklist(blacklist);

        HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(this.ruleData), handle);

        SelectorData selectorData = mock(SelectorData.class);
        when(this.chain.execute(any())).thenReturn(Mono.empty());

        StepVerifier.create(headerBlacklistPlugin.doExecute(exchange, this.chain, selectorData, this.ruleData))
                .expectSubscription()
                .verifyComplete();
    }

    @Test
    public void testGetOrder() {
        assertEquals(10, headerBlacklistPlugin.getOrder());
    }

    @Test
    public void testNamed() {
        assertEquals("header-blacklist", headerBlacklistPlugin.named());
    }
}
