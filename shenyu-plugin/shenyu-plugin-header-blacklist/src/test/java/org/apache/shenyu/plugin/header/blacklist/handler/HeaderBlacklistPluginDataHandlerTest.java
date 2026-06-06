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

package org.apache.shenyu.plugin.header.blacklist.handler;

import org.apache.shenyu.common.constant.Constants;
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.utils.GsonUtils;
import org.apache.shenyu.plugin.base.utils.CacheKeyUtils;
import org.apache.shenyu.plugin.header.blacklist.rule.HeaderBlacklistHandle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public final class HeaderBlacklistPluginDataHandlerTest {

    private HeaderBlacklistPluginDataHandler handler;

    @BeforeEach
    public void setUp() {
        handler = new HeaderBlacklistPluginDataHandler();
    }

    @Test
    public void testHandlerSelector() {
        SelectorData selectorData = new SelectorData();
        selectorData.setId("selector-id");
        selectorData.setContinued(false);
        selectorData.setHandle(buildHandleJson("DENY"));
        handler.handlerSelector(selectorData);
        HeaderBlacklistHandle cached = HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().obtainHandle(CacheKeyUtils.INST.getKey(selectorData.getId(), Constants.DEFAULT_RULE));
        assertNotNull(cached);
        assertEquals("DENY", cached.getDefaultStrategy());
        handler.removeSelector(selectorData);
        assertNull(HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().obtainHandle(CacheKeyUtils.INST.getKey(selectorData.getId(), Constants.DEFAULT_RULE)));
    }

    @Test
    public void testHandlerRule() {
        RuleData ruleData = new RuleData();
        ruleData.setId("rule-id");
        ruleData.setSelectorId("selector-id");
        ruleData.setHandle(buildHandleJson("ALLOW"));
        handler.handlerRule(ruleData);
        HeaderBlacklistHandle cached = HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().obtainHandle(CacheKeyUtils.INST.getKey(ruleData));
        assertNotNull(cached);
        assertEquals("ALLOW", cached.getDefaultStrategy());
        handler.removeRule(ruleData);
        assertNull(HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().obtainHandle(CacheKeyUtils.INST.getKey(ruleData)));
    }

    @Test
    public void testInvalidHandleFallback() {
        RuleData ruleData = new RuleData();
        ruleData.setId("rule-invalid");
        ruleData.setSelectorId("selector-id");
        ruleData.setHandle("{invalid-json}");
        handler.handlerRule(ruleData);
        HeaderBlacklistHandle cached = HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().obtainHandle(CacheKeyUtils.INST.getKey(ruleData));
        assertNotNull(cached);
        assertEquals("ALLOW", cached.getDefaultStrategy());
    }

    @Test
    public void testPluginNamed() {
        assertEquals("header-blacklist", handler.pluginNamed());
    }

    private String buildHandleJson(final String defaultStrategy) {
        HeaderBlacklistHandle handle = new HeaderBlacklistHandle();
        handle.setDefaultStrategy(defaultStrategy);
        HeaderBlacklistHandle.HeaderRule headerRule = new HeaderBlacklistHandle.HeaderRule();
        headerRule.setHeaderName("X-Token");
        headerRule.setWhitelist(Collections.singletonList("allowed-.*"));
        handle.setHeaderRules(Collections.singletonList(headerRule));
        return GsonUtils.getInstance().toJson(handle);
    }
}
