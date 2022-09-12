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

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Strings;
import org.apache.skywalking.oap.server.core.cluster.ClusterHealthStatus;
import org.apache.skywalking.oap.server.core.cluster.ClusterNodesQuery;
import org.apache.skywalking.oap.server.core.cluster.ClusterRegister;
import org.apache.skywalking.oap.server.core.cluster.OAPNodeChecker;
import org.apache.skywalking.oap.server.core.cluster.RemoteInstance;
import org.apache.skywalking.oap.server.core.cluster.ServiceQueryException;
import org.apache.skywalking.oap.server.core.cluster.ServiceRegisterException;
import org.apache.skywalking.oap.server.core.remote.client.Address;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;
import org.apache.skywalking.oap.server.library.util.CollectionUtils;
import org.apache.skywalking.oap.server.telemetry.TelemetryModule;
import org.apache.skywalking.oap.server.telemetry.api.HealthCheckMetrics;
import org.apache.skywalking.oap.server.telemetry.api.MetricsCreator;
import org.apache.skywalking.oap.server.telemetry.api.MetricsTag;

public class NacosCoordinator implements ClusterRegister, ClusterNodesQuery {

    private final ModuleDefineHolder manager;
    private final NamingService namingService;
    private final ClusterModuleNacosConfig config;
    private volatile Address selfAddress;
    private HealthCheckMetrics healthChecker;

    public NacosCoordinator(final ModuleDefineHolder manager, final NamingService namingService, final ClusterModuleNacosConfig config) {
        this.manager = manager;
        this.namingService = namingService;
        this.config = config;
    }

    /**
     * 查询远程节点的实现
     * @return
     */
    @Override
    public List<RemoteInstance> queryRemoteNodes() {
        List<RemoteInstance> remoteInstances = new ArrayList<>();
        try {
            // 初始化健康检查
            initHealthChecker();
            // 命名服务api根据当前服务名，查找实例
            // com.alibaba.nacos.api.naming.NamingService.selectInstances(java.lang.String, boolean)
            // true代表健康节点
            List<Instance> instances = namingService.selectInstances(config.getServiceName(), true);
            if (CollectionUtils.isNotEmpty(instances)) {
                // 遍历实例
                instances.forEach(instance -> {
                    // 把ip port传入传入，构造一个address
                    Address address = new Address(instance.getIp(), instance.getPort(), false);
                    // 判断是否是当前oap节点
                    // 只要host和port相等，就会被标记为self
                    if (address.equals(selfAddress)) {
                        address.setSelf(true);
                    }
                    remoteInstances.add(new RemoteInstance(address));
                });
            }
            // 传入当前远端实例列表，利用OAPNodeChecker判断集群健康状态
            ClusterHealthStatus healthStatus = OAPNodeChecker.isHealth(remoteInstances);
            // 记录指标
            if (healthStatus.isHealth()) {
                this.healthChecker.health();
            } else {
                this.healthChecker.unHealth(healthStatus.getReason());
            }
        } catch (Throwable e) {
            healthChecker.unHealth(e);
            throw new ServiceQueryException(e.getMessage());
        }
        // 返回列表
        return remoteInstances;
    }

    // 注册到远端
    @Override
    public void registerRemote(RemoteInstance remoteInstance) throws ServiceRegisterException {
        // 判断是否使用内部地址
        // 优先使用internalXXX的配置，否则使用grpc的host和port
        if (needUsingInternalAddr()) {
            // config中配置的internal相关的直接使用
            // TODO internalXXX配置的作用是什么？
            remoteInstance = new RemoteInstance(new Address(config.getInternalComHost(), config.getInternalComPort(), true));
        }
        // 获取host
        String host = remoteInstance.getAddress().getHost();
        // 获取ip
        int port = remoteInstance.getAddress().getPort();
        try {
            // 初始化健康检查
            initHealthChecker();
            // 像nacos注册实例
            namingService.registerInstance(config.getServiceName(), host, port);
            // 上报健康
            healthChecker.health();
        } catch (Throwable e) {
            healthChecker.unHealth(e);
            throw new ServiceRegisterException(e.getMessage());
        }
        // 讲当前地址类保存起来
        this.selfAddress = remoteInstance.getAddress();
    }

    // 是否使用内部地址
    private boolean needUsingInternalAddr() {
        return !Strings.isNullOrEmpty(config.getInternalComHost()) && config.getInternalComPort() > 0;
    }

    private void initHealthChecker() {
        // 如果健康检查为空，初始化一个HealthCheckMetrics的实例
        // TODO 初始化健康检查器的作用是什么
        if (healthChecker == null) {
            MetricsCreator metricCreator = manager.find(TelemetryModule.NAME).provider().getService(MetricsCreator.class);
            healthChecker = metricCreator.createHealthCheckerGauge("cluster_nacos", MetricsTag.EMPTY_KEY, MetricsTag.EMPTY_VALUE);
        }
    }
}
