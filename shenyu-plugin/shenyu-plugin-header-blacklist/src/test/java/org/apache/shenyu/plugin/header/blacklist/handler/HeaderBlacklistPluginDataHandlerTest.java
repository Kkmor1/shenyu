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

import org.apache.shenyu.common.dto.PluginData;
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.enums.PluginEnum;
import org.apache.shenyu.common.utils.GsonUtils;
import org.apache.shenyu.plugin.base.utils.CacheKeyUtils;
import org.apache.shenyu.plugin.header.blacklist.handle.HeaderBlacklistRuleHandle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Test case for {@link HeaderBlacklistPluginDataHandler}.
 */
public final class HeaderBlacklistPluginDataHandlerTest {

    private HeaderBlacklistPluginDataHandler handler;

    @BeforeEach
    public void setUp() {
        handler = new HeaderBlacklistPluginDataHandler();
    }

    @Test
    public void testPluginNamed() {
        assertEquals(PluginEnum.HEADER_BLACKLIST.getName(), handler.pluginNamed());
    }

    @Test
    public void testHandlerPlugin() {
        PluginData pluginData = new PluginData();
        pluginData.setId("plugin1");
        pluginData.setName("headerBlacklist");
        pluginData.setEnabled(true);
        pluginData.setConfig("{}");

        handler.handlerPlugin(pluginData);
    }

    @Test
    public void testRemovePlugin() {
        PluginData pluginData = new PluginData();
        pluginData.setId("plugin1");
        pluginData.setName("headerBlacklist");

        handler.removePlugin(pluginData);
    }

    @Test
    public void testHandlerSelector() {
        SelectorData selectorData = new SelectorData();
        selectorData.setId("selector1");
        selectorData.setName("test-selector");

        handler.handlerSelector(selectorData);
    }

    @Test
    public void testRemoveSelector() {
        SelectorData selectorData = new SelectorData();
        selectorData.setId("selector1");
        selectorData.setName("test-selector");

        handler.removeSelector(selectorData);
    }

    @Test
    public void testHandlerRule() {
        RuleData ruleData = new RuleData();
        ruleData.setId("rule1");
        ruleData.setSelectorId("selector1");
        ruleData.setPluginName("headerBlacklist");

        List<HeaderBlacklistRuleHandle.HeaderRule> headerRules = new ArrayList<>();
        HeaderBlacklistRuleHandle.HeaderRule headerRule = new HeaderBlacklistRuleHandle.HeaderRule();
        headerRule.setHeaderName("User-Agent");
        headerRule.setPattern(".*BadBot.*");
        headerRules.add(headerRule);

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setModel("blacklist");
        handle.setHeaderRules(headerRules);

        ruleData.setHandle(GsonUtils.getInstance().toJson(handle));

        handler.handlerRule(ruleData);

        String cacheKey = CacheKeyUtils.INST.getKey(ruleData);
        HeaderBlacklistRuleHandle cachedHandle = HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().obtainHandle(cacheKey);
        assertNotNull(cachedHandle);
        assertEquals("blacklist", cachedHandle.getModel());
        assertNotNull(cachedHandle.getHeaderRules());
        assertEquals(1, cachedHandle.getHeaderRules().size());
        assertEquals("User-Agent", cachedHandle.getHeaderRules().get(0).getHeaderName());
        assertEquals(".*BadBot.*", cachedHandle.getHeaderRules().get(0).getPattern());
    }

    @Test
    public void testHandlerRuleWithNullHandle() {
        RuleData ruleData = new RuleData();
        ruleData.setId("rule2");
        ruleData.setSelectorId("selector2");
        ruleData.setPluginName("headerBlacklist");
        ruleData.setHandle(null);

        handler.handlerRule(ruleData);
    }

    @Test
    public void testRemoveRule() {
        RuleData ruleData = new RuleData();
        ruleData.setId("rule3");
        ruleData.setSelectorId("selector3");
        ruleData.setPluginName("headerBlacklist");

        List<HeaderBlacklistRuleHandle.HeaderRule> headerRules = new ArrayList<>();
        HeaderBlacklistRuleHandle.HeaderRule headerRule = new HeaderBlacklistRuleHandle.HeaderRule();
        headerRule.setHeaderName("X-Api-Key");
        headerRule.setPattern("valid-.*");
        headerRules.add(headerRule);

        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setModel("whitelist");
        handle.setHeaderRules(headerRules);

        ruleData.setHandle(GsonUtils.getInstance().toJson(handle));

        handler.handlerRule(ruleData);

        String cacheKey = CacheKeyUtils.INST.getKey(ruleData);
        HeaderBlacklistRuleHandle cachedHandle = HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().obtainHandle(cacheKey);
        assertNotNull(cachedHandle);

        handler.removeRule(ruleData);

        HeaderBlacklistRuleHandle removedHandle = HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().obtainHandle(cacheKey);
        assertNull(removedHandle);
    }

    @Test
    public void testRemoveRuleWithNullHandle() {
        RuleData ruleData = new RuleData();
        ruleData.setId("rule4");
        ruleData.setSelectorId("selector4");
        ruleData.setPluginName("headerBlacklist");
        ruleData.setHandle(null);

        handler.removeRule(ruleData);
    }
}
