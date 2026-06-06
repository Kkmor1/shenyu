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

import org.apache.commons.lang3.StringUtils;
import org.apache.shenyu.common.constant.Constants;
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.utils.GsonUtils;
import org.apache.shenyu.plugin.base.cache.CommonHandleCache;
import org.apache.shenyu.plugin.base.handler.PluginDataHandler;
import org.apache.shenyu.plugin.base.utils.BeanHolder;
import org.apache.shenyu.plugin.base.utils.CacheKeyUtils;
import org.apache.shenyu.plugin.header.blacklist.rule.HeaderBlacklistHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class HeaderBlacklistPluginDataHandler implements PluginDataHandler {

    public static final String PLUGIN_NAME = "header-blacklist";

    public static final Supplier<CommonHandleCache<String, HeaderBlacklistHandle>> CACHED_HANDLE = new BeanHolder<>(CommonHandleCache::new);

    private static final Logger LOG = LoggerFactory.getLogger(HeaderBlacklistPluginDataHandler.class);

    @Override
    public void handlerSelector(final SelectorData selectorData) {
        if (!selectorData.getContinued()) {
            CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(selectorData.getId(), Constants.DEFAULT_RULE), parseHandle(selectorData.getHandle()));
        }
    }

    @Override
    public void removeSelector(final SelectorData selectorData) {
        CACHED_HANDLE.get().removeHandle(CacheKeyUtils.INST.getKey(selectorData.getId(), Constants.DEFAULT_RULE));
    }

    @Override
    public void handlerRule(final RuleData ruleData) {
        CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(ruleData), parseHandle(ruleData.getHandle()));
    }

    @Override
    public void removeRule(final RuleData ruleData) {
        CACHED_HANDLE.get().removeHandle(CacheKeyUtils.INST.getKey(ruleData));
    }

    @Override
    public String pluginNamed() {
        return PLUGIN_NAME;
    }

    private HeaderBlacklistHandle parseHandle(final String handleJson) {
        if (StringUtils.isBlank(handleJson)) {
            return HeaderBlacklistHandle.newDefaultInstance();
        }
        try {
            HeaderBlacklistHandle handle = GsonUtils.getInstance().fromJson(handleJson, HeaderBlacklistHandle.class);
            return handle == null ? HeaderBlacklistHandle.newDefaultInstance() : handle;
        } catch (Exception ex) {
            LOG.error("failed to parse header blacklist handle: {}", handleJson, ex);
            return HeaderBlacklistHandle.newDefaultInstance();
        }
    }
}
