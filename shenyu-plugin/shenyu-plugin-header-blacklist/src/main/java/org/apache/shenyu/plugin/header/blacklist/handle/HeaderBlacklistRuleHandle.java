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

package org.apache.shenyu.plugin.header.blacklist.handle;

import java.io.Serializable;
import java.util.List;

/**
 * The type Header blacklist rule handle.
 */
public class HeaderBlacklistRuleHandle implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The model: blacklist or whitelist.
     */
    private String model;

    /**
     * The header rules list.
     */
    private List<HeaderRule> headerRules;

    /**
     * Gets model.
     *
     * @return the model
     */
    public String getModel() {
        return model;
    }

    /**
     * Sets model.
     *
     * @param model the model
     */
    public void setModel(final String model) {
        this.model = model;
    }

    /**
     * Gets header rules.
     *
     * @return the header rules
     */
    public List<HeaderRule> getHeaderRules() {
        return headerRules;
    }

    /**
     * Sets header rules.
     *
     * @param headerRules the header rules
     */
    public void setHeaderRules(final List<HeaderRule> headerRules) {
        this.headerRules = headerRules;
    }

    /**
     * The type Header rule.
     */
    public static class HeaderRule implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The header name.
         */
        private String headerName;

        /**
         * The regex pattern to match header value.
         */
        private String pattern;

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
    }
}
