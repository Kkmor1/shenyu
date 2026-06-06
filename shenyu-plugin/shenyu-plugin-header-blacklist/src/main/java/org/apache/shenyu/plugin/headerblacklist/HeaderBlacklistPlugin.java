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

package org.apache.shenyu.plugin.headerblacklist;

import org.apache.shenyu.common.constant.Constants;
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.dto.convert.rule.HeaderBlacklistHandle;
import org.apache.shenyu.common.enums.PluginEnum;
import org.apache.shenyu.plugin.api.ShenyuPluginChain;
import org.apache.shenyu.plugin.api.result.ShenyuResultWrap;
import org.apache.shenyu.plugin.api.utils.WebFluxResultUtils;
import org.apache.shenyu.plugin.base.AbstractShenyuPlugin;
import org.apache.shenyu.plugin.base.utils.CacheKeyUtils;
import org.apache.shenyu.plugin.headerblacklist.handler.HeaderBlacklistPluginDataHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Header blacklist plugin.
 */
public class HeaderBlacklistPlugin extends AbstractShenyuPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(HeaderBlacklistPlugin.class);

    @Override
    protected Mono<Void> doExecute(final ServerWebExchange exchange, final ShenyuPluginChain chain,
                                    final SelectorData selector, final RuleData rule) {
        HeaderBlacklistHandle handle = buildRuleHandle(rule);
        if (Objects.isNull(handle)) {
            return chain.execute(exchange);
        }

        ServerHttpRequest request = exchange.getRequest();
        
        if (matchBlacklist(request, handle.getBlacklist())) {
            LOG.warn("Request header matches blacklist, path: {}", request.getURI().getPath());
            exchange.getResponse().setStatusCode(HttpStatus.FORBIDDEN);
            Object error = ShenyuResultWrap.error(exchange, HttpStatus.FORBIDDEN.value(), Constants.REJECT_MSG, null);
            return WebFluxResultUtils.result(exchange, error);
        }

        if (matchWhitelist(request, handle.getWhitelist())) {
            return chain.execute(exchange);
        }

        String defaultStrategy = handle.getDefaultStrategy();
        if ("deny".equalsIgnoreCase(defaultStrategy)) {
            LOG.warn("Request header not matched, default strategy deny, path: {}", request.getURI().getPath());
            exchange.getResponse().setStatusCode(HttpStatus.FORBIDDEN);
            Object error = ShenyuResultWrap.error(exchange, HttpStatus.FORBIDDEN.value(), Constants.REJECT_MSG, null);
            return WebFluxResultUtils.result(exchange, error);
        }

        return chain.execute(exchange);
    }

    @Override
    public String named() {
        return PluginEnum.HEADER_BLACKLIST.getName();
    }

    @Override
    public int getOrder() {
        return PluginEnum.HEADER_BLACKLIST.getCode();
    }

    private HeaderBlacklistHandle buildRuleHandle(final RuleData ruleData) {
        return HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get()
                .obtainHandle(CacheKeyUtils.INST.getKey(ruleData));
    }

    private boolean matchBlacklist(final ServerHttpRequest request,
                                    final List<HeaderBlacklistHandle.HeaderPattern> blacklist) {
        if (blacklist == null || blacklist.isEmpty()) {
            return false;
        }
        return blacklist.stream().anyMatch(pattern -> matchHeader(request, pattern));
    }

    private boolean matchWhitelist(final ServerHttpRequest request,
                                    final List<HeaderBlacklistHandle.HeaderPattern> whitelist) {
        if (whitelist == null || whitelist.isEmpty()) {
            return false;
        }
        return whitelist.stream().anyMatch(pattern -> matchHeader(request, pattern));
    }

    private boolean matchHeader(final ServerHttpRequest request,
                                 final HeaderBlacklistHandle.HeaderPattern headerPattern) {
        if (headerPattern == null || headerPattern.getHeaderName() == null) {
            return false;
        }
        List<String> headerValues = request.getHeaders().get(headerPattern.getHeaderName());
        if (headerValues == null || headerValues.isEmpty()) {
            return false;
        }
        String pattern = headerPattern.getPattern();
        if (pattern == null || pattern.isEmpty()) {
            return true;
        }
        Pattern regex = Pattern.compile(pattern);
        return headerValues.stream().anyMatch(value -> regex.matcher(value).matches());
    }
}
