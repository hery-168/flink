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

package org.apache.flink.runtime.jobgraph;

/**
 * The ScheduleMode decides how tasks of an execution graph are started.
 */
public enum ScheduleMode {
	/** Schedule tasks lazily from the sources. Downstream tasks are started once their input data are ready */
	// HeryCode:用于批处理
	LAZY_FROM_SOURCES(true),

	/**
	 * Same as LAZY_FROM_SOURCES just with the difference that it uses batch slot requests which support the
	 * execution of jobs with fewer slots than requested. However, the user needs to make sure that the job
	 * does not contain any pipelined shuffles (every pipelined region can be executed with a single slot).
	 */
// HeryCode:和上面区别是在该模式下，使用批处理资源申请模式，可以在资源不足的情况下执行作业，但要确保本阶段作业中没有shuffle
	LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST(true),

	/** Schedules all tasks immediately. */
	// HeryCode:用于流计算，一次申请需要的所有资源
	EAGER(false);

	private final boolean allowLazyDeployment;

	ScheduleMode(boolean allowLazyDeployment) {
		this.allowLazyDeployment = allowLazyDeployment;
	}

	/**
	 * Returns whether we are allowed to deploy consumers lazily.
	 */
	public boolean allowLazyDeployment() {
		return allowLazyDeployment;
	}
}
