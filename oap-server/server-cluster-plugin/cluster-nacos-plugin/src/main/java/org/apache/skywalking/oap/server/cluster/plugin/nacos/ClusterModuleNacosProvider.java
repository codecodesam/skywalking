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

package org.apache.skywalking.oap.server.cluster.plugin.nacos;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import java.util.Properties;

import org.apache.skywalking.oap.server.library.util.StringUtil;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.cluster.ClusterModule;
import org.apache.skywalking.oap.server.core.cluster.ClusterNodesQuery;
import org.apache.skywalking.oap.server.core.cluster.ClusterRegister;
import org.apache.skywalking.oap.server.library.module.ModuleConfig;
import org.apache.skywalking.oap.server.library.module.ModuleDefine;
import org.apache.skywalking.oap.server.library.module.ModuleProvider;
import org.apache.skywalking.oap.server.library.module.ModuleStartException;
import org.apache.skywalking.oap.server.library.module.ServiceNotProvidedException;

/**
 * 集群模块 nacos提供者
 */
public class ClusterModuleNacosProvider extends ModuleProvider {

    private final ClusterModuleNacosConfig config;
    // nacos命名服务的api接口
    // 由下面prepare负责初始化
    private NamingService namingService;

    public ClusterModuleNacosProvider() {
        super();
        this.config = new ClusterModuleNacosConfig();
    }

    @Override
    public String name() {
        return "nacos";
    }

    @Override
    public Class<? extends ModuleDefine> module() {
        return ClusterModule.class;
    }

    @Override
    public ModuleConfig createConfigBeanIfAbsent() {
        return config;
    }

    // 以集群发现实现nacos相关的准备工作为例子来学习prepare
    @Override
    public void prepare() throws ServiceNotProvidedException, ModuleStartException {
        try {
            Properties properties = new Properties();
            properties.put(PropertyKeyConst.SERVER_ADDR, config.getHostPort());
            properties.put(PropertyKeyConst.NAMESPACE, config.getNamespace());
            if (StringUtil.isNotEmpty(config.getUsername()) && StringUtil.isNotEmpty(config.getAccessKey())) {
                throw new ModuleStartException("Nacos Auth method should choose either username or accessKey, not both");
            }
            if (StringUtil.isNotEmpty(config.getUsername())) {
                properties.put(PropertyKeyConst.USERNAME, config.getUsername());
                properties.put(PropertyKeyConst.PASSWORD, config.getPassword());
            } else if (StringUtil.isNotEmpty(config.getAccessKey())) {
                properties.put(PropertyKeyConst.ACCESS_KEY, config.getAccessKey());
                properties.put(PropertyKeyConst.SECRET_KEY, config.getSecretKey());
            }
            namingService = NamingFactory.createNamingService(properties);
        } catch (Exception e) {
            throw new ModuleStartException(e.getMessage(), e);
        }
        // 将配置，命名服务，当前模块管理器传入，创建一个NacosCoordinator实例
        NacosCoordinator coordinator = new NacosCoordinator(getManager(), namingService, config);
        // 注册 集群注册的实现
        this.registerServiceImplementation(ClusterRegister.class, coordinator);
        // 注册 集群发现的实现
        this.registerServiceImplementation(ClusterNodesQuery.class, coordinator);
    }

    // 集群注册模块，nacos的实现没有做什么处理
    @Override
    public void start() throws ServiceNotProvidedException {
    }

    // 所有的provider都完成了start，boostrap flow对provides的回调工作
    @Override
    public void notifyAfterCompleted() throws ServiceNotProvidedException {

    }

    // 依赖核心模块
    // 主要是provider在进行start的一个先后顺序
    @Override
    public String[] requiredModules() {
        return new String[] {CoreModule.NAME};
    }
}
