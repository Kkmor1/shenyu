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
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.plugin.api.ShenyuPluginChain;
import org.apache.shenyu.plugin.api.result.ShenyuResultWrap;
import org.apache.shenyu.plugin.api.utils.WebFluxResultUtils;
import org.apache.shenyu.plugin.base.AbstractShenyuPlugin;
import org.apache.shenyu.plugin.base.utils.CacheKeyUtils;
import org.apache.shenyu.plugin.header.blacklist.config.HeaderBlacklistRuleHandle;
import org.apache.shenyu.plugin.header.blacklist.handler.HeaderBlacklistPluginDataHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class HeaderBlacklistPlugin extends AbstractShenyuPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(HeaderBlacklistPlugin.class);

    private static final String DEFAULT_POLICY_ALLOW = "allow";

    @Override
    protected Mono<Void> doExecute(final ServerWebExchange exchange, final ShenyuPluginChain chain,
                                   final SelectorData selector, final RuleData rule) {
        HeaderBlacklistRuleHandle ruleHandle = HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get()
                .obtainHandle(CacheKeyUtils.INST.getKey(rule));

        if (Objects.isNull(ruleHandle)) {
            LOG.warn("header blacklist rule handle not found for rule: {}", rule.getId());
            return chain.execute(exchange);
        }

        HttpHeaders headers = exchange.getRequest().getHeaders();

        if (matchWhitelist(ruleHandle.getWhitelistHeaders(), headers)) {
            return chain.execute(exchange);
        }

        if (matchBlacklist(ruleHandle.getBlacklistHeaders(), headers)) {
            exchange.getResponse().setStatusCode(HttpStatus.FORBIDDEN);
            Object error = ShenyuResultWrap.error(exchange, HttpStatus.FORBIDDEN.value(),
                    "Request blocked by header blacklist", null);
            return WebFluxResultUtils.result(exchange, error);
        }

        if (DEFAULT_POLICY_ALLOW.equals(ruleHandle.getDefaultPolicy())) {
            return chain.execute(exchange);
        }

        exchange.getResponse().setStatusCode(HttpStatus.FORBIDDEN);
        Object error = ShenyuResultWrap.error(exchange, HttpStatus.FORBIDDEN.value(),
                "Request blocked by header blacklist default policy", null);
        return WebFluxResultUtils.result(exchange, error);
    }

    private boolean matchWhitelist(final List<String> whitelistHeaders, final HttpHeaders headers) {
        if (whitelistHeaders == null || whitelistHeaders.isEmpty()) {
            return false;
        }
        return headers.keySet().stream().anyMatch(headerName -> matchesAnyPattern(headerName, whitelistHeaders));
    }

    private boolean matchBlacklist(final List<String> blacklistHeaders, final HttpHeaders headers) {
        if (blacklistHeaders == null || blacklistHeaders.isEmpty()) {
            return false;
        }
        return headers.keySet().stream().anyMatch(headerName -> matchesAnyPattern(headerName, blacklistHeaders));
    }

    private boolean matchesAnyPattern(final String headerName, final List<String> patterns) {
        for (String pattern : patterns) {
            try {
                if (Pattern.matches(pattern, headerName)) {
                    return true;
                }
            } catch (PatternSyntaxException e) {
                LOG.warn("Invalid regex pattern: {}", pattern, e);
            }
        }
        return false;
    }

    @Override
    public String named() {
        return "headerBlacklist";
    }

    @Override
    public int getOrder() {
        return 10;
    }
}