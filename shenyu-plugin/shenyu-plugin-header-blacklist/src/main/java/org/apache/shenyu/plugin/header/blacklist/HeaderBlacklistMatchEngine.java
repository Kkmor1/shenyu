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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.shenyu.plugin.header.blacklist.matcher.HeaderValueMatcher;
import org.apache.shenyu.plugin.header.blacklist.rule.HeaderBlacklistHandle;
import org.apache.shenyu.spi.ExtensionLoader;
import org.springframework.http.HttpHeaders;

import java.util.List;

public final class HeaderBlacklistMatchEngine {

    private HeaderBlacklistMatchEngine() {
    }

    public static HeaderMatchDecision match(final HttpHeaders headers, final HeaderBlacklistHandle handle) {
        HeaderValueMatcher matcher = ExtensionLoader.getExtensionLoader(HeaderValueMatcher.class).getJoin(handle.getMatcherName());
        boolean whitelistMatched = false;
        for (HeaderBlacklistHandle.HeaderRule headerRule : handle.getHeaderRules()) {
            if (StringUtils.isBlank(headerRule.getHeaderName())) {
                continue;
            }
            List<String> headerValues = headers.get(headerRule.getHeaderName());
            if (CollectionUtils.isEmpty(headerValues)) {
                continue;
            }
            if (matches(matcher, headerRule.getBlacklist(), headerValues)) {
                return HeaderMatchDecision.BLOCKED;
            }
            if (matches(matcher, headerRule.getWhitelist(), headerValues)) {
                whitelistMatched = true;
            }
        }
        if (whitelistMatched || handle.isDefaultAllow()) {
            return HeaderMatchDecision.ALLOWED;
        }
        return HeaderMatchDecision.BLOCKED;
    }

    private static boolean matches(final HeaderValueMatcher matcher, final List<String> patterns, final List<String> headerValues) {
        if (CollectionUtils.isEmpty(patterns) || CollectionUtils.isEmpty(headerValues)) {
            return false;
        }
        for (String pattern : patterns) {
            if (StringUtils.isBlank(pattern)) {
                continue;
            }
            for (String headerValue : headerValues) {
                if (StringUtils.isNotBlank(headerValue) && matcher.match(pattern, headerValue)) {
                    return true;
                }
            }
        }
        return false;
    }
}
