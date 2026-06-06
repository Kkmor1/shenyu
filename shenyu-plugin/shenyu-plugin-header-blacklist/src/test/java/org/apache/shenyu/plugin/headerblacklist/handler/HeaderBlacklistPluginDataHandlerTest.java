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

package org.apache.shenyu.plugin.headerblacklist.handler;

import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.dto.convert.rule.HeaderBlacklistHandle;
import org.apache.shenyu.common.enums.PluginEnum;
import org.apache.shenyu.common.utils.GsonUtils;
import org.apache.shenyu.plugin.base.utils.CacheKeyUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Test case for {@link HeaderBlacklistPluginDataHandler}.
 */
public final class HeaderBlacklistPluginDataHandlerTest {

    private HeaderBlacklistPluginDataHandler headerBlacklistPluginDataHandlerUnderTest;

    @BeforeEach
    public void setUp() {
        headerBlacklistPluginDataHandlerUnderTest = new HeaderBlacklistPluginDataHandler();
    }

    @Test
    public void testHandlerRule() {
        RuleData ruleData = new RuleData();
        ruleData.setId("headerBlacklistRule");
        ruleData.setSelectorId("headerBlacklist");

        HeaderBlacklistHandle handle = new HeaderBlacklistHandle();
        HeaderBlacklistHandle.HeaderPattern pattern =
                new HeaderBlacklistHandle.HeaderPattern("X-Test", ".*");
        handle.setBlacklist(Collections.singletonList(pattern));
        handle.setDefaultStrategy("allow");

        ruleData.setHandle(GsonUtils.getGson().toJson(handle));
        headerBlacklistPluginDataHandlerUnderTest.handlerRule(ruleData);

        HeaderBlacklistHandle cachedHandle =
                HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get()
                        .obtainHandle(CacheKeyUtils.INST.getKey(ruleData));
        assertEquals(handle.getDefaultStrategy(), cachedHandle.getDefaultStrategy());
        assertEquals(handle.getBlacklist().size(), cachedHandle.getBlacklist().size());

        headerBlacklistPluginDataHandlerUnderTest.removeRule(ruleData);
        assertNull(HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get()
                .obtainHandle(CacheKeyUtils.INST.getKey(ruleData)));
    }

    @Test
    public void testPluginNamed() {
        final String result = headerBlacklistPluginDataHandlerUnderTest.pluginNamed();
        assertEquals(PluginEnum.HEADER_BLACKLIST.getName(), result);
    }
}
