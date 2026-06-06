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

package org.apache.shenyu.plugin.header.blacklist.rule;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class HeaderBlacklistHandle implements Serializable {

    private static final long serialVersionUID = -5700622475656798839L;

    private String defaultStrategy = "ALLOW";

    private String matcher = "regex";

    private List<HeaderRule> headerRules = new ArrayList<>();

    public static HeaderBlacklistHandle newDefaultInstance() {
        return new HeaderBlacklistHandle();
    }

    public boolean isDefaultAllow() {
        return !"DENY".equalsIgnoreCase(defaultStrategy);
    }

    public String getMatcherName() {
        return StringUtils.defaultIfBlank(matcher, "regex").toLowerCase(Locale.ROOT);
    }

    public List<HeaderRule> getHeaderRules() {
        if (CollectionUtils.isEmpty(headerRules)) {
            return Collections.emptyList();
        }
        return headerRules;
    }

    public void setHeaderRules(final List<HeaderRule> headerRules) {
        this.headerRules = headerRules;
    }

    public String getDefaultStrategy() {
        return defaultStrategy;
    }

    public void setDefaultStrategy(final String defaultStrategy) {
        this.defaultStrategy = defaultStrategy;
    }

    public String getMatcher() {
        return matcher;
    }

    public void setMatcher(final String matcher) {
        this.matcher = matcher;
    }

    public static class HeaderRule implements Serializable {

        private static final long serialVersionUID = 6261454891098100015L;

        private String headerName;

        private List<String> whitelist = new ArrayList<>();

        private List<String> blacklist = new ArrayList<>();

        public String getHeaderName() {
            return headerName;
        }

        public void setHeaderName(final String headerName) {
            this.headerName = headerName;
        }

        public List<String> getWhitelist() {
            if (CollectionUtils.isEmpty(whitelist)) {
                return Collections.emptyList();
            }
            return whitelist;
        }

        public void setWhitelist(final List<String> whitelist) {
            this.whitelist = whitelist;
        }

        public List<String> getBlacklist() {
            if (CollectionUtils.isEmpty(blacklist)) {
                return Collections.emptyList();
            }
            return blacklist;
        }

        public void setBlacklist(final List<String> blacklist) {
            this.blacklist = blacklist;
        }
    }
}
