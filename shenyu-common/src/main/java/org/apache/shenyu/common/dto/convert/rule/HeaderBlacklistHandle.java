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

package org.apache.shenyu.common.dto.convert.rule;

import java.util.List;
import java.util.Objects;

/**
 * this is header blacklist plugin handle.
 */
public class HeaderBlacklistHandle {

    /**
     * blacklisted headers with regex patterns.
     */
    private List<HeaderPattern> blacklist;

    /**
     * whitelisted headers with regex patterns.
     */
    private List<HeaderPattern> whitelist;

    /**
     * default strategy when no match: "allow" or "deny".
     */
    private String defaultStrategy;

    /**
     * New default instance header blacklist handle.
     *
     * @return the header blacklist handle
     */
    public static HeaderBlacklistHandle newDefaultInstance() {
        HeaderBlacklistHandle handle = new HeaderBlacklistHandle();
        handle.setDefaultStrategy("allow");
        return handle;
    }

    /**
     * Gets blacklist.
     *
     * @return the blacklist
     */
    public List<HeaderPattern> getBlacklist() {
        return blacklist;
    }

    /**
     * Sets blacklist.
     *
     * @param blacklist the blacklist
     */
    public void setBlacklist(final List<HeaderPattern> blacklist) {
        this.blacklist = blacklist;
    }

    /**
     * Gets whitelist.
     *
     * @return the whitelist
     */
    public List<HeaderPattern> getWhitelist() {
        return whitelist;
    }

    /**
     * Sets whitelist.
     *
     * @param whitelist the whitelist
     */
    public void setWhitelist(final List<HeaderPattern> whitelist) {
        this.whitelist = whitelist;
    }

    /**
     * Gets default strategy.
     *
     * @return the default strategy
     */
    public String getDefaultStrategy() {
        return defaultStrategy;
    }

    /**
     * Sets default strategy.
     *
     * @param defaultStrategy the default strategy
     */
    public void setDefaultStrategy(final String defaultStrategy) {
        this.defaultStrategy = defaultStrategy;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (Objects.isNull(o) || getClass() != o.getClass()) {
            return false;
        }
        HeaderBlacklistHandle that = (HeaderBlacklistHandle) o;
        return Objects.equals(blacklist, that.blacklist)
                && Objects.equals(whitelist, that.whitelist)
                && Objects.equals(defaultStrategy, that.defaultStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(blacklist, whitelist, defaultStrategy);
    }

    @Override
    public String toString() {
        return "HeaderBlacklistHandle{"
                + "blacklist="
                + blacklist
                + ", whitelist="
                + whitelist
                + ", defaultStrategy='"
                + defaultStrategy
                + '\''
                + '}';
    }

    /**
     * Header pattern.
     */
    public static class HeaderPattern {

        /**
         * header name.
         */
        private String headerName;

        /**
         * regex pattern for header value.
         */
        private String pattern;

        /**
         * Default constructor.
         */
        public HeaderPattern() {
        }

        /**
         * Constructor with headerName and pattern.
         *
         * @param headerName the header name
         * @param pattern    the pattern
         */
        public HeaderPattern(final String headerName, final String pattern) {
            this.headerName = headerName;
            this.pattern = pattern;
        }

        /**
         * Gets header name.
         *
         * @return the header name
         */
        public String getHeaderName() {
            return headerName;
        }

        /**
         * Sets header name.
         *
         * @param headerName the header name
         */
        public void setHeaderName(final String headerName) {
            this.headerName = headerName;
        }

        /**
         * Gets pattern.
         *
         * @return the pattern
         */
        public String getPattern() {
            return pattern;
        }

        /**
         * Sets pattern.
         *
         * @param pattern the pattern
         */
        public void setPattern(final String pattern) {
            this.pattern = pattern;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (Objects.isNull(o) || getClass() != o.getClass()) {
                return false;
            }
            HeaderPattern that = (HeaderPattern) o;
            return Objects.equals(headerName, that.headerName)
                    && Objects.equals(pattern, that.pattern);
        }

        @Override
        public int hashCode() {
            return Objects.hash(headerName, pattern);
        }

        @Override
        public String toString() {
            return "HeaderPattern{"
                    + "headerName='"
                    + headerName
                    + '\''
                    + ", pattern='"
                    + pattern
                    + '\''
                    + '}';
        }
    }
}
