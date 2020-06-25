/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import java.util.Collections;

/**
 * Payload for heartbeats sent from the TaskExecutor to the JobManager.
 */
public class TaskExecutorToJobManagerHeartbeatPayload {

	private final AccumulatorReport accumulatorReport;

	private final ExecutionDeploymentReport executionDeploymentReport;

	public TaskExecutorToJobManagerHeartbeatPayload(AccumulatorReport accumulatorReport, ExecutionDeploymentReport executionDeploymentReport) {
		this.accumulatorReport = accumulatorReport;
		this.executionDeploymentReport = executionDeploymentReport;
	}

	public AccumulatorReport getAccumulatorReport() {
		return accumulatorReport;
	}

	public ExecutionDeploymentReport getExecutionDeploymentReport() {
		return executionDeploymentReport;
	}

	public static TaskExecutorToJobManagerHeartbeatPayload empty() {
		return new TaskExecutorToJobManagerHeartbeatPayload(
			new AccumulatorReport(Collections.emptyList()),
			new ExecutionDeploymentReport(Collections.emptySet()));
	}

	@Override
	public String toString() {
		return "TaskExectorToJobManagerHeartbeatPayload{" +
			"accumulatorReport=" + accumulatorReport +
			", executionDeploymentReport=" + executionDeploymentReport +
			'}';
	}
}
