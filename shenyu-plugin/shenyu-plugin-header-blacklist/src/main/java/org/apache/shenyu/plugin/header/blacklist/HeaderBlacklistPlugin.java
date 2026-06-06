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
import org.apache.shenyu.plugin.header.blacklist.handler.HeaderBlacklistPluginDataHandler;
import org.apache.shenyu.plugin.header.blacklist.rule.HeaderBlacklistHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.Objects;

public class HeaderBlacklistPlugin extends AbstractShenyuPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(HeaderBlacklistPlugin.class);

    @Override
    protected Mono<Void> doExecute(final ServerWebExchange exchange, final ShenyuPluginChain chain, final SelectorData selector,
            final RuleData rule) {
        HeaderBlacklistHandle handle = HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().obtainHandle(CacheKeyUtils.INST.getKey(rule));
        if (Objects.isNull(handle)) {
            return chain.execute(exchange);
        }
        try {
            HeaderMatchDecision decision = HeaderBlacklistMatchEngine.match(exchange.getRequest().getHeaders(), handle);
            if (HeaderMatchDecision.BLOCKED == decision) {
                return forbidden(exchange);
            }
            return chain.execute(exchange);
        } catch (Exception ex) {
            LOG.error("header blacklist plugin execute error, ruleId:{}", rule.getId(), ex);
            return forbidden(exchange);
        }
    }

    @Override
    public int getOrder() {
        return 10;
    }

    @Override
    public String named() {
        return HeaderBlacklistPluginDataHandler.PLUGIN_NAME;
    }

    @Override
    public boolean skip(final ServerWebExchange exchange) {
        return skipExceptHttpLike(exchange);
    }

    private Mono<Void> forbidden(final ServerWebExchange exchange) {
        exchange.getResponse().setStatusCode(HttpStatus.FORBIDDEN);
        Object error = ShenyuResultWrap.error(exchange, HttpStatus.FORBIDDEN.value(), HttpStatus.FORBIDDEN.getReasonPhrase(), null);
        return WebFluxResultUtils.result(exchange, error);
    }
}
