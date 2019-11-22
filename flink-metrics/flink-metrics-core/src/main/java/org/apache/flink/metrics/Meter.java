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

package org.apache.flink.metrics;

/**
 * Metric for measuring throughput.
 */
//这个表示度量，一般用于描述速率，比如多长时间里数据的输出速率等
public interface Meter extends Metric {

	/**
	 * Mark occurrence of an event.
	 */
	// 标记一个事件的发生
	void markEvent();

	/**
	 * Mark occurrence of multiple events.
	 *
	 * @param n number of events occurred
	 */
	//// 标记n个事件的发生
	void markEvent(long n);

	/**
	 * Returns the current rate of events per second.
	 *
	 * @return current rate of events per second
	 */
	// 返回速率
	double getRate();

	/**
	 * Get number of events marked on the meter.
	 *
	 * @return number of events marked on the meter
	 */
//	获取event数量
	long getCount();
}
