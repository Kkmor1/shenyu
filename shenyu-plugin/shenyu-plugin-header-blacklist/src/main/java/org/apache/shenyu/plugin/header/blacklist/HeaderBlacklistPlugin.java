package org.apache.shenyu.plugin.header.blacklist;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.plugin.api.ShenyuPluginChain;
import org.apache.shenyu.plugin.api.result.ShenyuResultWrap;
import org.apache.shenyu.plugin.api.utils.WebFluxResultUtils;
import org.apache.shenyu.plugin.base.AbstractShenyuPlugin;
import org.apache.shenyu.plugin.base.utils.CacheKeyUtils;
import org.apache.shenyu.plugin.header.blacklist.dto.HeaderBlacklistRuleHandle;
import org.apache.shenyu.plugin.header.blacklist.handler.HeaderBlacklistPluginDataHandler;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * The type Header blacklist plugin.
 */
public class HeaderBlacklistPlugin extends AbstractShenyuPlugin {

    @Override
    protected Mono<Void> doExecute(final ServerWebExchange exchange, final ShenyuPluginChain chain,
                                   final SelectorData selector, final RuleData rule) {
        HeaderBlacklistRuleHandle ruleHandle = HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get()
                .obtainHandle(CacheKeyUtils.INST.getKey(rule));

        if (Objects.isNull(ruleHandle)) {
            return chain.execute(exchange);
        }

        HttpHeaders headers = exchange.getRequest().getHeaders();

        // 1. Check whitelist first
        if (CollectionUtils.isNotEmpty(ruleHandle.getWhitelist()) && isMatch(headers, ruleHandle.getWhitelist())) {
            return chain.execute(exchange);
        }

        // 2. Check blacklist
        if (CollectionUtils.isNotEmpty(ruleHandle.getBlacklist()) && isMatch(headers, ruleHandle.getBlacklist())) {
            exchange.getResponse().setStatusCode(HttpStatus.FORBIDDEN);
            Object error = ShenyuResultWrap.error(exchange, HttpStatus.FORBIDDEN.value(), "Forbidden: Header Blacklist Matched", null);
            return WebFluxResultUtils.result(exchange, error);
        }

        // 3. Default strategy
        return chain.execute(exchange);
    }

    private boolean isMatch(final HttpHeaders headers, final List<HeaderBlacklistRuleHandle.MatchRule> rules) {
        for (HeaderBlacklistRuleHandle.MatchRule rule : rules) {
            String headerName = rule.getHeader();
            String regex = rule.getRegex();
            if (StringUtils.isNotBlank(headerName) && StringUtils.isNotBlank(regex)) {
                List<String> values = headers.get(headerName);
                if (CollectionUtils.isNotEmpty(values)) {
                    Pattern pattern = Pattern.compile(regex);
                    for (String value : values) {
                        if (pattern.matcher(value).matches()) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    @Override
    public int getOrder() {
        return 10;
    }

    @Override
    public String named() {
        return "header-blacklist";
    }
}
