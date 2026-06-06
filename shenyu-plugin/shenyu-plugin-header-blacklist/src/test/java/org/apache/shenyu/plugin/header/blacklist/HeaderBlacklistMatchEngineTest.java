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

import org.apache.shenyu.plugin.header.blacklist.rule.HeaderBlacklistHandle;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public final class HeaderBlacklistMatchEngineTest {

    @Test
    public void testBlacklistMatch() {
        HeaderBlacklistHandle handle = new HeaderBlacklistHandle();
        handle.setHeaderRules(Collections.singletonList(buildRule("X-Token", null, Collections.singletonList("blocked-.*"))));
        HttpHeaders headers = new HttpHeaders();
        headers.add("X-Token", "blocked-value");
        assertEquals(HeaderMatchDecision.BLOCKED, HeaderBlacklistMatchEngine.match(headers, handle));
    }

    @Test
    public void testWhitelistMatch() {
        HeaderBlacklistHandle handle = new HeaderBlacklistHandle();
        handle.setDefaultStrategy("DENY");
        handle.setHeaderRules(Collections.singletonList(buildRule("X-Token", Collections.singletonList("allowed-.*"), null)));
        HttpHeaders headers = new HttpHeaders();
        headers.add("X-Token", "allowed-value");
        assertEquals(HeaderMatchDecision.ALLOWED, HeaderBlacklistMatchEngine.match(headers, handle));
    }

    @Test
    public void testDefaultStrategyWhenUnmatched() {
        HeaderBlacklistHandle handle = new HeaderBlacklistHandle();
        handle.setDefaultStrategy("DENY");
        handle.setHeaderRules(Collections.singletonList(buildRule("X-Token", Collections.singletonList("allowed-.*"), null)));
        HttpHeaders headers = new HttpHeaders();
        headers.add("X-Token", "other-value");
        assertEquals(HeaderMatchDecision.BLOCKED, HeaderBlacklistMatchEngine.match(headers, handle));
    }

    @Test
    public void testInvalidRegexThrowsException() {
        HeaderBlacklistHandle handle = new HeaderBlacklistHandle();
        handle.setHeaderRules(Collections.singletonList(buildRule("X-Token", null, Collections.singletonList("[invalid")));
        HttpHeaders headers = new HttpHeaders();
        headers.add("X-Token", "blocked-value");
        assertThrows(RuntimeException.class, () -> HeaderBlacklistMatchEngine.match(headers, handle));
    }

    private HeaderBlacklistHandle.HeaderRule buildRule(final String headerName, final java.util.List<String> whitelist,
            final java.util.List<String> blacklist) {
        HeaderBlacklistHandle.HeaderRule headerRule = new HeaderBlacklistHandle.HeaderRule();
        headerRule.setHeaderName(headerName);
        headerRule.setWhitelist(whitelist);
        headerRule.setBlacklist(blacklist);
        return headerRule;
    }
}
