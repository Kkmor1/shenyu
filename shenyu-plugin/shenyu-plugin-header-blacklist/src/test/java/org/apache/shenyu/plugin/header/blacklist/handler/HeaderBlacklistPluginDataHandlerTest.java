package org.apache.shenyu.plugin.header.blacklist.handler;

import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.plugin.base.utils.CacheKeyUtils;
import org.apache.shenyu.plugin.header.blacklist.dto.HeaderBlacklistRuleHandle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The type Header blacklist plugin data handler test.
 */
public class HeaderBlacklistPluginDataHandlerTest {

    private HeaderBlacklistPluginDataHandler headerBlacklistPluginDataHandler;

    @BeforeEach
    public void setUp() {
        headerBlacklistPluginDataHandler = new HeaderBlacklistPluginDataHandler();
    }

    @Test
    public void testHandlerRule() {
        RuleData ruleData = new RuleData();
        ruleData.setId("test-rule-id");
        ruleData.setSelectorId("test-selector-id");
        ruleData.setHandle("{\"blacklist\":[{\"header\":\"X-Test\",\"regex\":\"^forbidden.*\"}],\"whitelist\":[]}");

        headerBlacklistPluginDataHandler.handlerRule(ruleData);

        HeaderBlacklistRuleHandle handle = HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().obtainHandle(CacheKeyUtils.INST.getKey(ruleData));
        assertNotNull(handle);
        assertEquals(1, handle.getBlacklist().size());
        assertEquals("X-Test", handle.getBlacklist().get(0).getHeader());
        assertEquals("^forbidden.*", handle.getBlacklist().get(0).getRegex());
    }

    @Test
    public void testRemoveRule() {
        RuleData ruleData = new RuleData();
        ruleData.setId("test-rule-id");
        ruleData.setSelectorId("test-selector-id");
        ruleData.setHandle("{\"blacklist\":[{\"header\":\"X-Test\",\"regex\":\"^forbidden.*\"}],\"whitelist\":[]}");

        headerBlacklistPluginDataHandler.handlerRule(ruleData);
        assertNotNull(HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().obtainHandle(CacheKeyUtils.INST.getKey(ruleData)));

        headerBlacklistPluginDataHandler.removeRule(ruleData);
        assertNull(HeaderBlacklistPluginDataHandler.CACHED_HANDLE.get().obtainHandle(CacheKeyUtils.INST.getKey(ruleData)));
    }

    @Test
    public void testPluginNamed() {
        assertEquals("header-blacklist", headerBlacklistPluginDataHandler.pluginNamed());
    }
}
