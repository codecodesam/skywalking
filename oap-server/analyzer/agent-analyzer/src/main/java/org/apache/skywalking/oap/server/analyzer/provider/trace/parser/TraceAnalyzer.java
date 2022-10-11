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

package org.apache.skywalking.oap.server.analyzer.provider.trace.parser;

import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.apm.network.language.agent.v3.SegmentObject;
import org.apache.skywalking.apm.network.language.agent.v3.SpanObject;
import org.apache.skywalking.apm.network.language.agent.v3.SpanType;
import org.apache.skywalking.oap.server.analyzer.provider.AnalyzerModuleConfig;
import org.apache.skywalking.oap.server.analyzer.provider.trace.parser.listener.AnalysisListener;
import org.apache.skywalking.oap.server.analyzer.provider.trace.parser.listener.EntryAnalysisListener;
import org.apache.skywalking.oap.server.analyzer.provider.trace.parser.listener.ExitAnalysisListener;
import org.apache.skywalking.oap.server.analyzer.provider.trace.parser.listener.FirstAnalysisListener;
import org.apache.skywalking.oap.server.analyzer.provider.trace.parser.listener.LocalAnalysisListener;
import org.apache.skywalking.oap.server.analyzer.provider.trace.parser.listener.SegmentListener;
import org.apache.skywalking.oap.server.library.module.ModuleManager;

@Slf4j
@RequiredArgsConstructor
public class TraceAnalyzer {
    private final ModuleManager moduleManager;
    private final SegmentParserListenerManager listenerManager;
    private final AnalyzerModuleConfig config;
    private List<AnalysisListener> analysisListeners = new ArrayList<>();

    public void doAnalysis(SegmentObject segmentObject) {
        // 如果当前对象块列表为空的话返回
        // span是一个块节点的概念，比如一个链路中10跳，那么最少10个span，如果每个链路中callback有2层并且都收集了，
        // 那么span就是10*2
        if (segmentObject.getSpansList().size() == 0) {
            return;
        }
        // 创建span的监听器
        createSpanListeners();
        // 通知segment监听器
        // 这一步已经把状态，时间什么的都算好了
        notifySegmentListener(segmentObject);
        // 对一个segment中的span集合做遍历
        segmentObject.getSpansList().forEach(spanObject -> {
            // 如果spanId是0的话，那就是第一个节点
            if (spanObject.getSpanId() == 0) {
                notifyFirstListener(spanObject, segmentObject);
            }
            // span的类型判断分支
            // 这个spanType具体的定义是什么
            //    enum SpanType {
            //    // Server side of RPC. Consumer side of MQ.
            //    Entry = 0;
            //    // Client side of RPC. Producer side of MQ.
            //    Exit = 1;
            //    // A common local code execution.
            //    Local = 2;
            //}
            if (SpanType.Exit.equals(spanObject.getSpanType())) {
                notifyExitListener(spanObject, segmentObject);
            } else if (SpanType.Entry.equals(spanObject.getSpanType())) {
                notifyEntryListener(spanObject, segmentObject);
            } else if (SpanType.Local.equals(spanObject.getSpanType())) {
                notifyLocalListener(spanObject, segmentObject);
            } else {
                log.error("span type value was unexpected, span type name: {}", spanObject.getSpanType()
                                                                                          .name());
            }
        });
        // 通知监听器去构建
        // 主要是让处理完的数据继续往下走而已
        // 有数据落盘等等
        notifyListenerToBuild();
    }

    private void notifyListenerToBuild() {
        analysisListeners.forEach(AnalysisListener::build);
    }

    private void notifyExitListener(SpanObject span, SegmentObject segmentObject) {
        analysisListeners.forEach(listener -> {
            if (listener.containsPoint(AnalysisListener.Point.Exit)) {
                ((ExitAnalysisListener) listener).parseExit(span, segmentObject);
            }
        });
    }

    private void notifyEntryListener(SpanObject span, SegmentObject segmentObject) {
        analysisListeners.forEach(listener -> {
            if (listener.containsPoint(AnalysisListener.Point.Entry)) {
                ((EntryAnalysisListener) listener).parseEntry(span, segmentObject);
            }
        });
    }

    private void notifyLocalListener(SpanObject span, SegmentObject segmentObject) {
        analysisListeners.forEach(listener -> {
            if (listener.containsPoint(AnalysisListener.Point.Local)) {
                ((LocalAnalysisListener) listener).parseLocal(span, segmentObject);
            }
        });
    }

    private void notifyFirstListener(SpanObject span, SegmentObject segmentObject) {
        analysisListeners.forEach(listener -> {
            // 这一部分 当前的实现更多是执行一些属性的解析设置而已
            if (listener.containsPoint(AnalysisListener.Point.First)) {
                ((FirstAnalysisListener) listener).parseFirst(span, segmentObject);
            }
        });
    }

    private void notifySegmentListener(SegmentObject segmentObject) {
        // 其实就是遍历分析器，做一个前置判断和一个解析
        analysisListeners.forEach(listener -> {
            // point的类型有5种，Entry, Exit, Local, First, Segment
            if (listener.containsPoint(AnalysisListener.Point.Segment)) {
                ((SegmentListener) listener).parseSegment(segmentObject);
            }
        });
    }

    private void createSpanListeners() {
        // 获取工厂遍历创建
        // SegmentAnalysisListener.Factory
        // RPCAnalysisListener.Factory
        // EndpointDepFromCrossThreadAnalysisListener.Factory
        // NetworkAddressAliasMappingListener.Factory
        listenerManager.getSpanListenerFactories()
                       .forEach(
                           spanListenerFactory -> analysisListeners.add(
                               spanListenerFactory.create(moduleManager, config)));
    }
}
