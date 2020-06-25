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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionDeploymentListener;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Default {@link ExecutionDeploymentTracker} implementation.
 */
public class ExecutionDeploymentTrackerImpl implements ExecutionDeploymentTracker, ExecutionDeploymentListener {

	private final Map<ResourceID, Set<ExecutionAttemptID>> executionsByHost = new HashMap<>();
	private final Map<ExecutionAttemptID, ResourceID> hostByExecution = new HashMap<>();

	@Override
	public void startTrackingDeployment(ExecutionAttemptID execution, ResourceID host) {
		hostByExecution.put(execution, host);
		executionsByHost.compute(host, (resourceID, executionAttemptIds) -> {
			if (executionAttemptIds == null) {
				executionAttemptIds = new HashSet<>();
			}
			executionAttemptIds.add(execution);
			return executionAttemptIds;
		});
	}

	@Override
	public void stopTrackingDeployment(ExecutionAttemptID execution) {
		ResourceID host = hostByExecution.remove(execution);
		if (host != null) {
			executionsByHost.computeIfPresent(host, (resourceID, executionAttemptIds) -> {
				executionAttemptIds.remove(execution);

				return executionAttemptIds.isEmpty()
					? null
					: executionAttemptIds;
			});
		}
	}

	@Override
	public Set<ExecutionAttemptID> getExecutions(ResourceID host) {
		return executionsByHost.getOrDefault(host, Collections.emptySet());
	}

	@Override
	public void onCompletedDeployment(ExecutionAttemptID execution, ResourceID host) {
		startTrackingDeployment(execution, host);
	}
}
