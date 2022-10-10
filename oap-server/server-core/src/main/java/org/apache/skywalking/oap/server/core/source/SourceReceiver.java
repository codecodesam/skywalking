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

package org.apache.skywalking.oap.server.core.source;

import org.apache.skywalking.oap.server.core.analysis.DispatcherDetectorListener;
import org.apache.skywalking.oap.server.library.module.Service;

/**
 * The source receiver implementation delegates to {@link org.apache.skywalking.oap.server.core.analysis.DispatcherManager}
 * in order to forward source to the suitable real {@link org.apache.skywalking.oap.server.core.analysis.SourceDispatcher}.
 */
// org.apache.skywalking.oap.server.core.source.SourceReceiver
// 也是core模块定义的一种服务
public interface SourceReceiver extends Service {
    // 接收
    void receive(ISource source);
    // 获取分发检测监听器
    DispatcherDetectorListener getDispatcherDetectorListener();
}
