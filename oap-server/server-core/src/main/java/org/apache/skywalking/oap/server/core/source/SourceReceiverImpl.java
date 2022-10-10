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

import java.io.IOException;
import lombok.Getter;
import org.apache.skywalking.oap.server.core.analysis.DispatcherDetectorListener;
import org.apache.skywalking.oap.server.core.analysis.DispatcherManager;

public class SourceReceiverImpl implements SourceReceiver {
    @Getter
    private final DispatcherManager dispatcherManager;

    public SourceReceiverImpl() {
        this.dispatcherManager = new DispatcherManager();
    }

    // 分发管理器去分发
    // 分发管理器维护者scope为key的一个分发器map
    @Override
    public void receive(ISource source) {
        dispatcherManager.forward(source);
    }

    @Override
    public DispatcherDetectorListener getDispatcherDetectorListener() {
        return getDispatcherManager();
    }

    // 分发器的扫描在core模块start的时候开始了
    // 直接扫skywalking包下的类，如果是SourceDispatcher一律都要
    // SourceDispatcher是一个范型接口，范型类被限制为ISource类型，可以得到scope
    public void scan() throws IOException, InstantiationException, IllegalAccessException {
        dispatcherManager.scan();
    }
}
