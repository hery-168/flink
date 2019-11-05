/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.runtime.state.InternalPriorityQueue;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link InternalTimerService} that stores timers on the Java heap.
 * 负责具体的时间服务操作
 */
public class InternalTimerServiceImpl<K, N> implements InternalTimerService<N>, ProcessingTimeCallback {

    private final ProcessingTimeService processingTimeService;

    private final KeyContext keyContext;

    /**
     * Processing time timers that are currently in-flight.
     * KeyGroupedInternalPriorityQueue：是一种flink自身实现的优先级队列
     * 存储的数据是TimerHeapInternalTimer类型，包含三个属性key/namespace/timestamp，在优先级队列中按照timestamp进行排序
     */
    private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> processingTimeTimersQueue;

    /**
     * Event time timers that are currently in-flight.
     */
    private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue;

    /**
     * Information concerning the local key-group range.
     */
    private final KeyGroupRange localKeyGroupRange;

    private final int localKeyGroupRangeStartIdx;

    /**
     * The local event time, as denoted by the last received
     * {@link org.apache.flink.streaming.api.watermark.Watermark Watermark}.
     */
    private long currentWatermark = Long.MIN_VALUE;

    /**
     * The one and only Future (if any) registered to execute the
     * next {@link Triggerable} action, when its (processing) time arrives.
     */
    private ScheduledFuture<?> nextTimer;

    // Variables to be set when the service is started.

    private TypeSerializer<K> keySerializer;

    private TypeSerializer<N> namespaceSerializer;

    private Triggerable<K, N> triggerTarget;

    private volatile boolean isInitialized;

    private TypeSerializer<K> keyDeserializer;

    private TypeSerializer<N> namespaceDeserializer;

    /**
     * The restored timers snapshot, if any.
     */
    private InternalTimersSnapshot<K, N> restoredTimersSnapshot;

    InternalTimerServiceImpl(
            KeyGroupRange localKeyGroupRange,
            KeyContext keyContext,
            ProcessingTimeService processingTimeService,
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> processingTimeTimersQueue,
            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue) {

        this.keyContext = checkNotNull(keyContext);
        this.processingTimeService = checkNotNull(processingTimeService);
        this.localKeyGroupRange = checkNotNull(localKeyGroupRange);
        this.processingTimeTimersQueue = checkNotNull(processingTimeTimersQueue);
        this.eventTimeTimersQueue = checkNotNull(eventTimeTimersQueue);

        // find the starting index of the local key-group range
        int startIdx = Integer.MAX_VALUE;
        for (Integer keyGroupIdx : localKeyGroupRange) {
            startIdx = Math.min(keyGroupIdx, startIdx);
        }
        this.localKeyGroupRangeStartIdx = startIdx;
    }

    /**
     * Starts the local {@link InternalTimerServiceImpl} by:
     * <ol>
     *     <li>Setting the {@code keySerialized} and {@code namespaceSerializer} for the timers it will contain.</li>
     *     <li>Setting the {@code triggerTarget} which contains the action to be performed when a timer fires.</li>
     *     <li>Re-registering timers that were retrieved after recovering from a node failure, if any.</li>
     * </ol>
     * This method can be called multiple times, as long as it is called with the same serializers.
     */
    // 主要用于时间状态恢复初始化
    public void startTimerService(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            Triggerable<K, N> triggerTarget) {

        if (!isInitialized) {

            if (keySerializer == null || namespaceSerializer == null) {
                throw new IllegalArgumentException("The TimersService serializers cannot be null.");
            }

            if (this.keySerializer != null || this.namespaceSerializer != null || this.triggerTarget != null) {
                throw new IllegalStateException("The TimerService has already been initialized.");
            }

            // the following is the case where we restore
            if (restoredTimersSnapshot != null) {
                TypeSerializerSchemaCompatibility<K> keySerializerCompatibility =
                        restoredTimersSnapshot.getKeySerializerSnapshot().resolveSchemaCompatibility(keySerializer);

                if (keySerializerCompatibility.isIncompatible() || keySerializerCompatibility.isCompatibleAfterMigration()) {
                    throw new IllegalStateException(
                            "Tried to initialize restored TimerService with new key serializer that requires migration or is incompatible.");
                }

                TypeSerializerSchemaCompatibility<N> namespaceSerializerCompatibility =
                        restoredTimersSnapshot.getNamespaceSerializerSnapshot().resolveSchemaCompatibility(namespaceSerializer);

                if (namespaceSerializerCompatibility.isIncompatible() || namespaceSerializerCompatibility.isCompatibleAfterMigration()) {
                    throw new IllegalStateException(
                            "Tried to initialize restored TimerService with new namespace serializer that requires migration or is incompatible.");
                }

                this.keySerializer = keySerializerCompatibility.isCompatibleAsIs()
                        ? keySerializer : keySerializerCompatibility.getReconfiguredSerializer();
                this.namespaceSerializer = namespaceSerializerCompatibility.isCompatibleAsIs()
                        ? namespaceSerializer : namespaceSerializerCompatibility.getReconfiguredSerializer();
            } else {
                this.keySerializer = keySerializer;
                this.namespaceSerializer = namespaceSerializer;
            }

            this.keyDeserializer = null;
            this.namespaceDeserializer = null;

            this.triggerTarget = Preconditions.checkNotNull(triggerTarget);

            // re-register the restored timers (if any)
            final InternalTimer<K, N> headTimer = processingTimeTimersQueue.peek();
            //processingTimeTimersQueue获取数据，如果不为空，那么就调用processingTimeService.registerTimer 重新注册定时器
            if (headTimer != null) {
                nextTimer = processingTimeService.registerTimer(headTimer.getTimestamp(), this);
            }
            this.isInitialized = true;
        } else {
            if (!(this.keySerializer.equals(keySerializer) && this.namespaceSerializer.equals(namespaceSerializer))) {
                throw new IllegalArgumentException("Already initialized Timer Service " +
                        "tried to be initialized with different key and namespace serializers.");
            }
        }
    }

    @Override
    public long currentProcessingTime() {
        return processingTimeService.getCurrentProcessingTime();
    }

    @Override
    public long currentWatermark() {
        return currentWatermark;
    }



	/**
	 * 注册处理时间定时器
	 * @param namespace
	 * namespace在普通的keyedStream 中namespace表示VoidNamespace
	 * 在WindowedStream中namespace表示的是Window对象
	 * @param time time表示的是注册的触发时间
	 *             在这个方法中，主要做两件事情
	 *             1、将namespace与time转换为InternalTimer存入KeyGroupedInternalPriorityQueue优先级队列中
	 *             其中key从当前的KeyContext中获取，如果该Queue中包含相同的key/namspace/time，将不会被添加进去并且不会执行下面调用
	 *             2、调用SystemProcessingTimeService.registerTimer方法，传入具体的时间参数time，在registerTimer方法中
	 *             使用ScheduledThreadPoolExecutor提交一个定时执行的方法，定时执行对象是实现Runnable接口的TriggerTask，
	 *             那么当达到执行时间就会执行里面的run方法
	 */
    @Override
    public void registerProcessingTimeTimer(N namespace, long time) {
        InternalTimer<K, N> oldHead = processingTimeTimersQueue.peek();
        /**在flink中，注册一个定时器，都是把相关信息封装成TimerHeapInternalTimer 对象，这个对象包含三个内容
         * time ,key,namespace
         * 在flink内部确定一个具体的状态的具体数据需要key/namespce，
         * 第一个具体代表的是operator/statedesc，由于TimerHeapInternalTimer需要容错所以同样包含key/namespace
         * 从另外一个角度也说明在一个operator中如果我们多次注册同一个key相同的时间，达到的效果是一样，
         * 只会触发一次(默认在flink内部namespce是相同的)，同样也说明了注册的定时器必须是在keyedStream中
         */
		if (processingTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace))) {
            long nextTriggerTime = oldHead != null ? oldHead.getTimestamp() : Long.MAX_VALUE;
            // check if we need to re-schedule our timer to earlier
            if (time < nextTriggerTime) {
                if (nextTimer != null) {
                    nextTimer.cancel(false);
                }
                nextTimer = processingTimeService.registerTimer(time, this);
            }
        }
    }

    // 注册事件时间定时器
    @Override
    public void registerEventTimeTimer(N namespace, long time) {
        eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
    }

    // 删除process 时间定时器
    @Override
    public void deleteProcessingTimeTimer(N namespace, long time) {
        processingTimeTimersQueue.remove(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
    }

    // 删除event  时间定时器
    @Override
    public void deleteEventTimeTimer(N namespace, long time) {
        eventTimeTimersQueue.remove(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
    }

    //实现ProcessingTimeCallback接口的回调方法，用于处理时间触发执行方法
    @Override
    public void onProcessingTime(long time) throws Exception {
        // null out the timer in case the Triggerable calls registerProcessingTimeTimer()
        // inside the callback.
        nextTimer = null;

        InternalTimer<K, N> timer;
		//循环遍历KeyGroupedInternalPriorityQueue这个优先级队列
		//如果获取到的时间小于调用的触发时间，那么就会执行Triggerable.onProcessingTime方法
		//Triggerable表示具体定时操作接口，例如WindowOperator/KeyedProcessOperator 都实现了该接口
        while ((timer = processingTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
            processingTimeTimersQueue.poll();
            keyContext.setCurrentKey(timer.getKey());
            triggerTarget.onProcessingTime(timer);
        }

        if (timer != null && nextTimer == null) {
            nextTimer = processingTimeService.registerTimer(timer.getTimestamp(), this);
        }
    }

    // 用于event 时间中
    public void advanceWatermark(long time) throws Exception {
        currentWatermark = time;

        InternalTimer<K, N> timer;

        while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
            eventTimeTimersQueue.poll();
            keyContext.setCurrentKey(timer.getKey());
            triggerTarget.onEventTime(timer);
        }
    }

    /**
     * Snapshots the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
     *
     * @param keyGroupIdx the id of the key-group to be put in the snapshot.
     * @return a snapshot containing the timers for the given key-group, and the serializers for them
     */
    // 对该timerservice进行checkpoint
    public InternalTimersSnapshot<K, N> snapshotTimersForKeyGroup(int keyGroupIdx) {
        return new InternalTimersSnapshot<>(
                keySerializer,
                namespaceSerializer,
                eventTimeTimersQueue.getSubsetForKeyGroup(keyGroupIdx),
                processingTimeTimersQueue.getSubsetForKeyGroup(keyGroupIdx));
    }

    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    /**
     * Restore the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
     *
     * @param restoredSnapshot the restored snapshot containing the key-group's timers,
     *                         and the serializers that were used to write them
     * @param keyGroupIdx      the id of the key-group to be put in the snapshot.
     */
    // 时间状态恢复
    @SuppressWarnings("unchecked")
    public void restoreTimersForKeyGroup(InternalTimersSnapshot<?, ?> restoredSnapshot, int keyGroupIdx) {
        this.restoredTimersSnapshot = (InternalTimersSnapshot<K, N>) restoredSnapshot;

        TypeSerializer<K> restoredKeySerializer = restoredTimersSnapshot.getKeySerializerSnapshot().restoreSerializer();
        if (this.keyDeserializer != null && !this.keyDeserializer.equals(restoredKeySerializer)) {
            throw new IllegalArgumentException("Tried to restore timers for the same service with different key serializers.");
        }
        this.keyDeserializer = restoredKeySerializer;

        TypeSerializer<N> restoredNamespaceSerializer = restoredTimersSnapshot.getNamespaceSerializerSnapshot().restoreSerializer();
        if (this.namespaceDeserializer != null && !this.namespaceDeserializer.equals(restoredNamespaceSerializer)) {
            throw new IllegalArgumentException("Tried to restore timers for the same service with different namespace serializers.");
        }
        this.namespaceDeserializer = restoredNamespaceSerializer;

        checkArgument(localKeyGroupRange.contains(keyGroupIdx),
                "Key Group " + keyGroupIdx + " does not belong to the local range.");

        // restore the event time timers
        eventTimeTimersQueue.addAll(restoredTimersSnapshot.getEventTimeTimers());

        // restore the processing time timers
        processingTimeTimersQueue.addAll(restoredTimersSnapshot.getProcessingTimeTimers());
    }

    @VisibleForTesting
    public int numProcessingTimeTimers() {
        return this.processingTimeTimersQueue.size();
    }

    @VisibleForTesting
    public int numEventTimeTimers() {
        return this.eventTimeTimersQueue.size();
    }

    @VisibleForTesting
    public int numProcessingTimeTimers(N namespace) {
        return countTimersInNamespaceInternal(namespace, processingTimeTimersQueue);
    }

    @VisibleForTesting
    public int numEventTimeTimers(N namespace) {
        return countTimersInNamespaceInternal(namespace, eventTimeTimersQueue);
    }

    private int countTimersInNamespaceInternal(N namespace, InternalPriorityQueue<TimerHeapInternalTimer<K, N>> queue) {
        int count = 0;
        try (final CloseableIterator<TimerHeapInternalTimer<K, N>> iterator = queue.iterator()) {
            while (iterator.hasNext()) {
                final TimerHeapInternalTimer<K, N> timer = iterator.next();
                if (timer.getNamespace().equals(namespace)) {
                    count++;
                }
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("Exception when closing iterator.", e);
        }
        return count;
    }

    @VisibleForTesting
    int getLocalKeyGroupRangeStartIdx() {
        return this.localKeyGroupRangeStartIdx;
    }

    @VisibleForTesting
    List<Set<TimerHeapInternalTimer<K, N>>> getEventTimeTimersPerKeyGroup() {
        return partitionElementsByKeyGroup(eventTimeTimersQueue);
    }

    @VisibleForTesting
    List<Set<TimerHeapInternalTimer<K, N>>> getProcessingTimeTimersPerKeyGroup() {
        return partitionElementsByKeyGroup(processingTimeTimersQueue);
    }

    private <T> List<Set<T>> partitionElementsByKeyGroup(KeyGroupedInternalPriorityQueue<T> keyGroupedQueue) {
        List<Set<T>> result = new ArrayList<>(localKeyGroupRange.getNumberOfKeyGroups());
        for (int keyGroup : localKeyGroupRange) {
            result.add(Collections.unmodifiableSet(keyGroupedQueue.getSubsetForKeyGroup(keyGroup)));
        }
        return result;
    }
}
