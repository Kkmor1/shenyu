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

import org.apache.commons.lang3.StringUtils;
import org.apache.shenyu.common.constant.Constants;
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.enums.PluginEnum;
import org.apache.shenyu.plugin.api.ShenyuPluginChain;
import org.apache.shenyu.plugin.api.result.ShenyuResultEnum;
import org.apache.shenyu.plugin.api.result.ShenyuResultWrap;
import org.apache.shenyu.plugin.api.utils.WebFluxResultUtils;
import org.apache.shenyu.plugin.base.AbstractShenyuPlugin;
import org.apache.shenyu.plugin.base.utils.CacheKeyUtils;
import org.apache.shenyu.plugin.header.blacklist.handle.HeaderBlacklistRuleHandle;
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

/**
 * Header Blacklist Plugin.
 * This plugin is used to filter requests based on request header blacklist/whitelist rules.
 */
public class HeaderBlacklistPlugin extends AbstractShenyuPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(HeaderBlacklistPlugin.class);

    private static final String BLACKLIST_MODEL = "blacklist";

    private static final String WHITELIST_MODEL = "whitelist";

    @Override
    protected Mono<Void> doExecute(final ServerWebExchange exchange, final ShenyuPluginChain chain, 
                                    final SelectorData selector, final RuleData rule) {
        HeaderBlacklistRuleHandle ruleHandle = HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get()
                .obtainHandle(CacheKeyUtils.INST.getKey(rule));

        if (Objects.isNull(ruleHandle) || Objects.isNull(ruleHandle.getHeaderRules()) 
                || ruleHandle.getHeaderRules().isEmpty()) {
            LOG.warn("header blacklist rule handle is null or empty, pass through");
            return chain.execute(exchange);
        }

        String model = ruleHandle.getModel();
        HttpHeaders headers = exchange.getRequest().getHeaders();
        List<HeaderBlacklistRuleHandle.HeaderRule> headerRules = ruleHandle.getHeaderRules();

        if (StringUtils.isBlank(model)) {
            LOG.warn("header blacklist model is not configured, pass through");
            return chain.execute(exchange);
        }

        boolean matched = isHeaderMatched(headers, headerRules);

        if (BLACKLIST_MODEL.equals(model)) {
            if (matched) {
                LOG.info("Request header matched blacklist, reject request");
                exchange.getResponse().setStatusCode(HttpStatus.FORBIDDEN);
                Object error = ShenyuResultWrap.error(exchange, 
                        ShenyuResultEnum.FORBIDDEN.getCode(), 
                        ShenyuResultEnum.FORBIDDEN.getMsg(), 
                        null);
                return WebFluxResultUtils.result(exchange, error);
            }
            return chain.execute(exchange);
        } else if (WHITELIST_MODEL.equals(model)) {
            if (matched) {
                return chain.execute(exchange);
            }
            LOG.info("Request header not matched whitelist, reject request");
            exchange.getResponse().setStatusCode(HttpStatus.FORBIDDEN);
            Object error = ShenyuResultWrap.error(exchange, 
                    ShenyuResultEnum.FORBIDDEN.getCode(), 
                    ShenyuResultEnum.FORBIDDEN.getMsg(), 
                    null);
            return WebFluxResultUtils.result(exchange, error);
        }

        LOG.warn("Unknown model type: {}, pass through", model);
        return chain.execute(exchange);
    }

    private boolean isHeaderMatched(final HttpHeaders headers, 
                                     final List<HeaderBlacklistRuleHandle.HeaderRule> headerRules) {
        for (HeaderBlacklistRuleHandle.HeaderRule rule : headerRules) {
            String headerName = rule.getHeaderName();
            String pattern = rule.getPattern();

            if (StringUtils.isBlank(headerName) || StringUtils.isBlank(pattern)) {
                continue;
            }

            List<String> headerValues = headers.get(headerName);
            if (Objects.isNull(headerValues) || headerValues.isEmpty()) {
                continue;
            }

            try {
                Pattern regexPattern = Pattern.compile(pattern);
                for (String headerValue : headerValues) {
                    if (regexPattern.matcher(headerValue).matches()) {
                        LOG.debug("Header {} value {} matched pattern {}", headerName, headerValue, pattern);
                        return true;
                    }
                }
            } catch (Exception e) {
                LOG.error("Invalid regex pattern: {} for header: {}", pattern, headerName, e);
            }
        }
        return false;
    }

    @Override
    public String named() {
        return PluginEnum.HEADER_BLACKLIST.getName();
    }

    @Override
    public int getOrder() {
        return PluginEnum.HEADER_BLACKLIST.getCode();
    }
}
