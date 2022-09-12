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
 *
 */

package org.apache.skywalking.oap.server.library.module;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * The <code>ModuleManager</code> takes charge of all {@link ModuleDefine}s in collector.
 */
public class ModuleManager implements ModuleDefineHolder {
    // 是否处于预准备阶段
    private boolean isInPrepareStage = true;
    // 加载的模块
    private final Map<String, ModuleDefine> loadedModules = new HashMap<>();

    /**
     * Init the given modules
     */
    public void init(
        ApplicationConfiguration applicationConfiguration) throws ModuleNotFoundException, ProviderNotFoundException, ServiceNotProvidedException, CycleDependencyException, ModuleConfigException, ModuleStartException {
        // 获取模块名列表
        String[] moduleNames = applicationConfiguration.moduleList();
        // 获取服务加载器 -- 模块定义
        // TODO ServiceLoader.load
        ServiceLoader<ModuleDefine> moduleServiceLoader = ServiceLoader.load(ModuleDefine.class);
        // 获取服务加载器 -- 模块提供者
        ServiceLoader<ModuleProvider> moduleProviderLoader = ServiceLoader.load(ModuleProvider.class);

        HashSet<String> moduleSet = new HashSet<>(Arrays.asList(moduleNames));
        // ServiceLoader实现了Iterable，所以是可迭代的
        for (ModuleDefine module : moduleServiceLoader) {
            // 先判断解析完毕后的配置中是否含有这个模块
            if (moduleSet.contains(module.name())) {
                // 调用模块的准备方法
                // 参数：模块管理器，模块配置，模块提供者加载器
                // 该方法是一个抽象父类中的方法
                module.prepare(this, applicationConfiguration.getModuleConfiguration(module.name()), moduleProviderLoader);
                // 把该模块纳入已经加载的模块
                loadedModules.put(module.name(), module);
                // 从集合中移除
                moduleSet.remove(module.name());
            }
        }
        // Finish prepare stage
        // 完成准备阶段，修改变量
        isInPrepareStage = false;
        // 如果集合大于0，代表有模块还未初始化
        if (moduleSet.size() > 0) {
            throw new ModuleNotFoundException(moduleSet.toString() + " missing.");
        }
        // 启动器流转对象
        BootstrapFlow bootstrapFlow = new BootstrapFlow(loadedModules);
        // 开始
        bootstrapFlow.start(this);
        // 通知调用当流转完成时
        bootstrapFlow.notifyAfterCompleted();
    }

    @Override
    public boolean has(String moduleName) {
        return loadedModules.get(moduleName) != null;
    }

    @Override
    public ModuleProviderHolder find(String moduleName) throws ModuleNotFoundRuntimeException {
        assertPreparedStage();
        ModuleDefine module = loadedModules.get(moduleName);
        if (module != null)
            return module;
        throw new ModuleNotFoundRuntimeException(moduleName + " missing.");
    }

    private void assertPreparedStage() {
        if (isInPrepareStage) {
            throw new AssertionError("Still in preparing stage.");
        }
    }
}
