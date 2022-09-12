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

package org.apache.skywalking.oap.server.starter.config;

import java.io.FileNotFoundException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.library.util.PropertyPlaceholderHelper;
import org.apache.skywalking.oap.server.library.module.ApplicationConfiguration;
import org.apache.skywalking.oap.server.library.module.ProviderNotFoundException;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.apache.skywalking.oap.server.library.util.ResourceUtils;
import org.yaml.snakeyaml.Yaml;

/**
 * Initialize collector settings with following sources. Use application.yml as primary setting, and fix missing setting
 * by default settings in application-default.yml.
 * <p>
 * At last, override setting by system.properties and system.envs if the key matches moduleName.provideName.settingKey.
 */
@Slf4j
public class ApplicationConfigLoader implements ConfigLoader<ApplicationConfiguration> {
    // -表示该配置忽略
    private static final String DISABLE_SELECTOR = "-";
    // 每个模块都有一个选择器
    private static final String SELECTOR = "selector";

    private final Yaml yaml = new Yaml();

    @Override
    public ApplicationConfiguration load() throws ConfigFileNotFoundException {
        // 创建应用配置
        // 这个应用配置本质上就是把所有selector聚合在一起
        ApplicationConfiguration configuration = new ApplicationConfiguration();
        // 记载配置
        this.loadConfig(configuration);
        // 通过环境变量覆盖配置
        this.overrideConfigBySystemEnv(configuration);
        return configuration;
    }

    @SuppressWarnings("unchecked")
    private void loadConfig(ApplicationConfiguration configuration) throws ConfigFileNotFoundException {
        try {
            // 读取application.yml
            Reader applicationReader = ResourceUtils.read("application.yml");
            Map<String, Map<String, Object>> moduleConfig = yaml.loadAs(applicationReader, Map.class);
            if (CollectionUtils.isNotEmpty(moduleConfig)) {
                // 遍历配置
                // 从这一步得到的是一个筛选完毕的配置文件
                // 如果配置有问题的话，里面已经报错了
                selectConfig(moduleConfig);
                // 遍历
                moduleConfig.forEach((moduleName, providerConfig) -> {
                    if (providerConfig.size() > 0) {
                        log.info("Get a module define from application.yml, module name: {}", moduleName);
                        // 添加模块
                        // 跟上面说的一样 就是创建一个name对应module配置
                        ApplicationConfiguration.ModuleConfiguration moduleConfiguration = configuration.addModule(
                            moduleName);
                        // 这里是遍历模块和它的属性
                        providerConfig.forEach((providerName, config) -> {
                            log.info(
                                "Get a provider define belong to {} module, provider name: {}", moduleName,
                                providerName
                            );
                            // providerName就是模块名，比如nacos
                            // config是一个key value集合
                            final Map<String, ?> propertiesConfig = (Map<String, ?>) config;
                            final Properties properties = new Properties();
                            if (propertiesConfig != null) {
                                // 遍历模块对应的key value属性值
                                propertiesConfig.forEach((propertyName, propertyValue) -> {
                                    if (propertyValue instanceof Map) {
                                        Properties subProperties = new Properties();
                                        ((Map) propertyValue).forEach((key, value) -> {
                                            subProperties.put(key, value);
                                            // 其实就是替代${}
                                            replacePropertyAndLog(key, value, subProperties, providerName);
                                        });
                                        properties.put(propertyName, subProperties);
                                    } else {
                                        properties.put(propertyName, propertyValue);
                                        replacePropertyAndLog(propertyName, propertyValue, properties, providerName);
                                    }
                                });
                            }
                            // 添加一个提供者配置
                            moduleConfiguration.addProviderConfiguration(providerName, properties);
                        });
                    } else {
                        log.warn(
                            "Get a module define from application.yml, but no provider define, use default, module name: {}",
                            moduleName
                        );
                    }
                });
            }
        } catch (FileNotFoundException e) {
            throw new ConfigFileNotFoundException(e.getMessage(), e);
        }
    }

    private void replacePropertyAndLog(final Object propertyName, final Object propertyValue, final Properties target,
                                       final Object providerName) {
        final String valueString = PropertyPlaceholderHelper.INSTANCE
            .replacePlaceholders(propertyValue + "", target);
        if (valueString != null) {
            if (valueString.trim().length() == 0) {
                target.replace(propertyName, valueString);
                log.info("Provider={} config={} has been set as an empty string", providerName, propertyName);
            } else {
                // Use YAML to do data type conversion.
                final Object replaceValue = convertValueString(valueString);
                if (replaceValue != null) {
                    target.replace(propertyName, replaceValue);
                    log.info(
                        "Provider={} config={} has been set as {}",
                        providerName,
                        propertyName,
                        replaceValue.toString()
                    );
                }
            }
        }
    }

    private Object convertValueString(String valueString) {
        try {
            Object replaceValue = yaml.load(valueString);
            if (replaceValue instanceof String || replaceValue instanceof Integer || replaceValue instanceof Long || replaceValue instanceof Boolean || replaceValue instanceof ArrayList) {
                return replaceValue;
            } else {
                return valueString;
            }
        } catch (Exception e) {
            log.warn("yaml convert value type error, use origin values string. valueString={}", valueString, e);
            return valueString;
        }
    }

    private void overrideConfigBySystemEnv(ApplicationConfiguration configuration) {
        for (Map.Entry<Object, Object> prop : System.getProperties().entrySet()) {
            overrideModuleSettings(configuration, prop.getKey().toString(), prop.getValue().toString());
        }
    }

    private void selectConfig(final Map<String, Map<String, Object>> moduleConfiguration) {
        Iterator<Map.Entry<String, Map<String, Object>>> moduleIterator = moduleConfiguration.entrySet().iterator();
        while (moduleIterator.hasNext()) {
            Map.Entry<String, Map<String, Object>> entry = moduleIterator.next();
            final String moduleName = entry.getKey();
            final Map<String, Object> providerConfig = entry.getValue();
            // 如果没有含有selector就不是一个需要被加载的配置
            if (!providerConfig.containsKey(SELECTOR)) {
                continue;
            }
            final String selector = (String) providerConfig.get(SELECTOR);
            // 通过系统变量去替换${}得到一个确定的字符串
            final String resolvedSelector = PropertyPlaceholderHelper.INSTANCE.replacePlaceholders(
                selector, System.getProperties()
            );
            // 一份模块配置里会有很多可选项目
            // 根据上面得到的选项进行排除其他
            providerConfig.entrySet().removeIf(e -> !resolvedSelector.equals(e.getKey()));

            // 不为空，筛选结束
            if (!providerConfig.isEmpty()) {
                continue;
            }
            // 如果最后得到为空，代表我的selector并没有存在对应的配置
            // 如果配置的selector不是-的话，代表你填写是错误的
            // 既没有对应的配置，又没有忽略加载
            if (!DISABLE_SELECTOR.equals(resolvedSelector)) {
                throw new ProviderNotFoundException(
                    "no provider found for module " + moduleName + ", " +
                        "if you're sure it's not required module and want to remove it, " +
                        "set the selector to -"
                );
            }

            // now the module can be safely removed
            moduleIterator.remove();
            log.info("Remove module {} without any provider", moduleName);
        }
    }

    private void overrideModuleSettings(ApplicationConfiguration configuration, String key, String value) {
        int moduleAndConfigSeparator = key.indexOf('.');
        if (moduleAndConfigSeparator <= 0) {
            return;
        }
        String moduleName = key.substring(0, moduleAndConfigSeparator);
        String providerSettingSubKey = key.substring(moduleAndConfigSeparator + 1);
        ApplicationConfiguration.ModuleConfiguration moduleConfiguration = configuration.getModuleConfiguration(
            moduleName);
        if (moduleConfiguration == null) {
            return;
        }
        int providerAndConfigSeparator = providerSettingSubKey.indexOf('.');
        if (providerAndConfigSeparator <= 0) {
            return;
        }
        String providerName = providerSettingSubKey.substring(0, providerAndConfigSeparator);
        String settingKey = providerSettingSubKey.substring(providerAndConfigSeparator + 1);
        if (!moduleConfiguration.has(providerName)) {
            return;
        }
        Properties providerSettings = moduleConfiguration.getProviderConfiguration(providerName);
        if (!providerSettings.containsKey(settingKey)) {
            return;
        }
        Object originValue = providerSettings.get(settingKey);
        Class<?> type = originValue.getClass();
        if (type.equals(int.class) || type.equals(Integer.class))
            providerSettings.put(settingKey, Integer.valueOf(value));
        else if (type.equals(String.class))
            providerSettings.put(settingKey, value);
        else if (type.equals(long.class) || type.equals(Long.class))
            providerSettings.put(settingKey, Long.valueOf(value));
        else if (type.equals(boolean.class) || type.equals(Boolean.class)) {
            providerSettings.put(settingKey, Boolean.valueOf(value));
        } else {
            return;
        }

        log.info(
            "The setting has been override by key: {}, value: {}, in {} provider of {} module through {}", settingKey,
            value, providerName, moduleName, "System.properties"
        );
    }
}
