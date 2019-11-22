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

package org.apache.flink.metrics.reporter;

import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;

/**
 * Reporters are used to export {@link Metric Metrics} to an external backend.
 *
 * <p>Reporters are instantiated via reflection and must be public, non-abstract, and have a
 * public no-argument constructor.
 */
public interface MetricReporter {

	// ------------------------------------------------------------------------
	//  life cycle
	// ------------------------------------------------------------------------

	/**
	 * Configures this reporter. Since reporters are instantiated generically and hence parameter-less,
	 * this method is the place where the reporters set their basic fields based on configuration values.
	 *
	 * <p>This method is always called first on a newly instantiated reporter.
	 *
	 * @param config A properties object that contains all parameters set for this reporter.
	 */
	// 进行初始化配置操作，由于子类都是用无参构造函数，通过反射进行实例化，
	// 所以相关初始化的工作都是放在这里进行的，并且这个方法需要在实例化后，就需要调用该方法进行相关初始化的工作
	void open(MetricConfig config);

	/**
	 * Closes this reporter. Should be used to close channels, streams and release resources.
	 */
	// 关闭reporter
	void close();

	// ------------------------------------------------------------------------
	//  adding / removing metrics
	// ------------------------------------------------------------------------

	/**
	 * Called when a new {@link Metric} was added.
	 *
	 * @param metric      the metric that was added
	 * @param metricName  the name of the metric
	 * @param group       the group that contains the metric
	 */
	// 当一个新的metric添加的时候，调用该方法
	void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group);

	/**
	 * Called when a {@link Metric} was should be removed.
	 *
	 * @param metric      the metric that should be removed
	 * @param metricName  the name of the metric
	 * @param group       the group that contains the metric
	 */
	// 当一个metric被移除的时候调用
	void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group);
}
