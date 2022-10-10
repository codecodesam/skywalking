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

package org.apache.skywalking.oap.server.core.analysis;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.skywalking.oap.server.core.UnexpectedException;
import org.apache.skywalking.oap.server.core.source.ISource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DispatcherManager implements DispatcherDetectorListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(DispatcherManager.class);

    private Map<Integer, List<SourceDispatcher>> dispatcherMap;

    public DispatcherManager() {
        this.dispatcherMap = new HashMap<>();
    }

    public void forward(ISource source) {
        if (source == null) {
            return;
        }

        List<SourceDispatcher> dispatchers = dispatcherMap.get(source.scope());

        /**
         * Dispatcher is only generated by oal script analysis result.
         * So these will/could be possible, the given source doesn't have the dispatcher,
         * when the receiver is open, and oal script doesn't ask for analysis.
         */
        // 这里注释表示分发器都是oal脚本分析结果生成的？？？？
        // 先调用source的准备方法
        if (dispatchers != null) {
            source.prepare();
            // 遍历分发器
            for (SourceDispatcher dispatcher : dispatchers) {
                dispatcher.dispatch(source);
            }
        }
    }

    /**
     * Scan all classes under `org.apache.skywalking` package,
     * <p>
     * If it implement {@link org.apache.skywalking.oap.server.core.analysis.SourceDispatcher}, then, it will be added
     * into this DispatcherManager based on the Source definition.
     */
    public void scan() throws IOException, IllegalAccessException, InstantiationException {
        ClassPath classpath = ClassPath.from(this.getClass().getClassLoader());
        ImmutableSet<ClassPath.ClassInfo> classes = classpath.getTopLevelClassesRecursive("org.apache.skywalking");
        for (ClassPath.ClassInfo classInfo : classes) {
            Class<?> aClass = classInfo.load();

            addIfAsSourceDispatcher(aClass);
        }
    }

    @Override
    public void addIfAsSourceDispatcher(Class aClass) throws IllegalAccessException, InstantiationException {
        if (!aClass.isInterface() && !Modifier.isAbstract(
            aClass.getModifiers()) && SourceDispatcher.class.isAssignableFrom(aClass)) {
            Type[] genericInterfaces = aClass.getGenericInterfaces();
            for (Type genericInterface : genericInterfaces) {
                ParameterizedType anInterface = (ParameterizedType) genericInterface;
                if (anInterface.getRawType().getTypeName().equals(SourceDispatcher.class.getName())) {
                    Type[] arguments = anInterface.getActualTypeArguments();

                    if (arguments.length != 1) {
                        throw new UnexpectedException("unexpected type argument number, class " + aClass.getName());
                    }
                    Type argument = arguments[0];

                    Object source = ((Class) argument).newInstance();

                    if (!ISource.class.isAssignableFrom(source.getClass())) {
                        throw new UnexpectedException(
                            "unexpected type argument of class " + aClass.getName() + ", should be `org.apache.skywalking.oap.server.core.source.Source`. ");
                    }

                    ISource dispatcherSource = (ISource) source;
                    SourceDispatcher dispatcher = (SourceDispatcher) aClass.newInstance();

                    int scopeId = dispatcherSource.scope();

                    List<SourceDispatcher> dispatchers = this.dispatcherMap.get(scopeId);
                    if (dispatchers == null) {
                        dispatchers = new ArrayList<>();
                        this.dispatcherMap.put(scopeId, dispatchers);
                    }

                    dispatchers.add(dispatcher);

                    LOGGER.info("Dispatcher {} is added into DefaultScopeDefine {}.", dispatcher.getClass()
                                                                                                .getName(), scopeId);
                }
            }
        }
    }
}
