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

import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.utils.GsonUtils;
import org.apache.shenyu.plugin.base.utils.CacheKeyUtils;
import org.apache.shenyu.plugin.header.blacklist.config.HeaderBlacklistRuleHandle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public final class HeaderBlacklistPluginDataHandlerTest {

    private HeaderBlacklistPluginDataHandler handler;

    private RuleData ruleData;

    @BeforeEach
    public void setUp() {
        handler = new HeaderBlacklistPluginDataHandler();
        ruleData = new RuleData();
        ruleData.setId("testRule");
        ruleData.setSelectorId("testSelector");
        ruleData.setPluginName("headerBlacklist");
    }

    @Test
    public void testPluginNamed() {
        assertEquals("headerBlacklist", handler.pluginNamed());
    }

    @Test
    public void testHandlerRule() {
        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setBlacklistHeaders(Arrays.asList("X-Bad-.*"));
        handle.setWhitelistHeaders(Arrays.asList("X-Good-.*"));
        handle.setDefaultPolicy("deny");
        ruleData.setHandle(GsonUtils.getInstance().toJson(handle));

        handler.handlerRule(ruleData);

        HeaderBlacklistRuleHandle cached = HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get()
                .obtainHandle(CacheKeyUtils.INST.getKey(ruleData));
        assertNotNull(cached);
        assertEquals(handle.getBlacklistHeaders(), cached.getBlacklistHeaders());
        assertEquals(handle.getWhitelistHeaders(), cached.getWhitelistHeaders());
        assertEquals(handle.getDefaultPolicy(), cached.getDefaultPolicy());
    }

    @Test
    public void testHandlerRuleWithNullHandle() {
        ruleData.setHandle(null);

        handler.handlerRule(ruleData);

        HeaderBlacklistRuleHandle cached = HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get()
                .obtainHandle(CacheKeyUtils.INST.getKey(ruleData));
        assertNull(cached);
    }

    @Test
    public void testRemoveRule() {
        HeaderBlacklistRuleHandle handle = new HeaderBlacklistRuleHandle();
        handle.setBlacklistHeaders(Collections.singletonList("X-.*"));
        ruleData.setHandle(GsonUtils.getInstance().toJson(handle));

        handler.handlerRule(ruleData);
        assertNotNull(HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get()
                .obtainHandle(CacheKeyUtils.INST.getKey(ruleData)));

        handler.removeRule(ruleData);
        assertNull(HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get()
                .obtainHandle(CacheKeyUtils.INST.getKey(ruleData)));
    }
}