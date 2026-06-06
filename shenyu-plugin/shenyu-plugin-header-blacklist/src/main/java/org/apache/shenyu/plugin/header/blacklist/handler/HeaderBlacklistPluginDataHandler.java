package org.apache.shenyu.plugin.header.blacklist.handler;

import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.utils.GsonUtils;
import org.apache.shenyu.plugin.base.cache.CommonHandleCache;
import org.apache.shenyu.plugin.base.handler.PluginDataHandler;
import org.apache.shenyu.plugin.base.utils.BeanHolder;
import org.apache.shenyu.plugin.base.utils.CacheKeyUtils;
import org.apache.shenyu.plugin.header.blacklist.dto.HeaderBlacklistRuleHandle;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * The type Header blacklist plugin data handler.
 */
public class HeaderBlacklistPluginDataHandler implements PluginDataHandler {

    public static final Supplier<CommonHandleCache<String, HeaderBlacklistRuleHandle>> CACHED_HANDLE = new BeanHolder<>(CommonHandleCache::new);

    @Override
    public void handlerRule(final RuleData ruleData) {
        Optional.ofNullable(ruleData.getHandle()).ifPresent(s -> {
            HeaderBlacklistRuleHandle ruleHandle = GsonUtils.getInstance().fromJson(s, HeaderBlacklistRuleHandle.class);
            CACHED_HANDLE.get().cachedHandle(CacheKeyUtils.INST.getKey(ruleData), ruleHandle);
        });
    }

    @Override
    public void removeRule(final RuleData ruleData) {
        Optional.ofNullable(ruleData).ifPresent(s -> CACHED_HANDLE.get().removeHandle(CacheKeyUtils.INST.getKey(ruleData)));
    }

    @Override
    public String pluginNamed() {
        return "header-blacklist";
    }
}
