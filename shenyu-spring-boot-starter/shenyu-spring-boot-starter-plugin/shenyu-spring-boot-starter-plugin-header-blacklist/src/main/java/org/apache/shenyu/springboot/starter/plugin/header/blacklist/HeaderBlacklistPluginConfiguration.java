package org.apache.shenyu.springboot.starter.plugin.header.blacklist;

import org.apache.shenyu.plugin.base.handler.PluginDataHandler;
import org.apache.shenyu.plugin.header.blacklist.HeaderBlacklistPlugin;
import org.apache.shenyu.plugin.header.blacklist.handler.HeaderBlacklistPluginDataHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The type Header blacklist plugin configuration.
 */
@Configuration
public class HeaderBlacklistPluginConfiguration {

    /**
     * Header blacklist plugin.
     *
     * @return the header blacklist plugin
     */
    @Bean
    public HeaderBlacklistPlugin headerBlacklistPlugin() {
        return new HeaderBlacklistPlugin();
    }

    /**
     * Header blacklist plugin data handler.
     *
     * @return the plugin data handler
     */
    @Bean
    public PluginDataHandler headerBlacklistPluginDataHandler() {
        return new HeaderBlacklistPluginDataHandler();
    }
}
