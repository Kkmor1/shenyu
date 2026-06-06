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

package org.apache.shenyu.springboot.starter.plugin.header.blacklist;

import org.apache.shenyu.plugin.api.ShenyuPlugin;
import org.apache.shenyu.plugin.base.handler.PluginDataHandler;
import org.apache.shenyu.plugin.header.blacklist.HeaderBlacklistPlugin;
import org.apache.shenyu.plugin.header.blacklist.handler.HeaderBlacklistPluginDataHandler;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test case for {@link HeaderBlacklistPluginConfiguration}.
 */
public final class HeaderBlacklistPluginConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(HeaderBlacklistPluginConfiguration.class));

    @Test
    public void testHeaderBlacklistPlugin() {
        this.contextRunner.run(context -> {
            ShenyuPlugin plugin = context.getBean("headerBlacklistPlugin", ShenyuPlugin.class);
            assertThat(plugin).isNotNull();
            assertThat(plugin).isInstanceOf(HeaderBlacklistPlugin.class);
        });
    }

    @Test
    public void testHeaderBlacklistPluginDataHandler() {
        this.contextRunner.run(context -> {
            PluginDataHandler handler = context.getBean("headerBlacklistPluginDataHandler", PluginDataHandler.class);
            assertThat(handler).isNotNull();
            assertThat(handler).isInstanceOf(HeaderBlacklistPluginDataHandler.class);
        });
    }
}
