package org.apache.shenyu.plugin.header.blacklist.dto;

import java.util.List;
import java.util.Objects;

/**
 * The type Header blacklist rule handle.
 */
public class HeaderBlacklistRuleHandle {

    private List<MatchRule> blacklist;

    private List<MatchRule> whitelist;

    /**
     * Gets blacklist.
     *
     * @return the blacklist
     */
    public List<MatchRule> getBlacklist() {
        return blacklist;
    }

    /**
     * Sets blacklist.
     *
     * @param blacklist the blacklist
     */
    public void setBlacklist(List<MatchRule> blacklist) {
        this.blacklist = blacklist;
    }

    /**
     * Gets whitelist.
     *
     * @return the whitelist
     */
    public List<MatchRule> getWhitelist() {
        return whitelist;
    }

    /**
     * Sets whitelist.
     *
     * @param whitelist the whitelist
     */
    public void setWhitelist(List<MatchRule> whitelist) {
        this.whitelist = whitelist;
    }

    /**
     * The type Match rule.
     */
    public static class MatchRule {
        private String header;
        private String regex;

        /**
         * Gets header.
         *
         * @return the header
         */
        public String getHeader() {
            return header;
        }

        /**
         * Sets header.
         *
         * @param header the header
         */
        public void setHeader(String header) {
            this.header = header;
        }

        /**
         * Gets regex.
         *
         * @return the regex
         */
        public String getRegex() {
            return regex;
        }

        /**
         * Sets regex.
         *
         * @param regex the regex
         */
        public void setRegex(String regex) {
            this.regex = regex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MatchRule matchRule = (MatchRule) o;
            return Objects.equals(header, matchRule.header) && Objects.equals(regex, matchRule.regex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(header, regex);
        }
    }
}
