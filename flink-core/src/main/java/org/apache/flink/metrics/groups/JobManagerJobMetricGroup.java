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
package org.apache.flink.metrics.groups;

import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.MetricRegistry;
import org.apache.flink.metrics.groups.scope.ScopeFormat.JobManagerJobScopeFormat;

import javax.annotation.Nullable;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing everything belonging to
 * a specific job, running on the JobManager.
 */
public class JobManagerJobMetricGroup extends JobMetricGroup {

	/** The metrics group that contains this group */
	private final JobManagerMetricGroup parent;

	public JobManagerJobMetricGroup(
		MetricRegistry registry,
		JobManagerMetricGroup parent,
		JobID jobId,
		@Nullable String jobName) {

		this(registry, checkNotNull(parent), registry.getScopeFormats().getJobManagerJobFormat(), jobId, jobName);
	}

	public JobManagerJobMetricGroup(
		MetricRegistry registry,
		JobManagerMetricGroup parent,
		JobManagerJobScopeFormat scopeFormat,
		JobID jobId,
		@Nullable String jobName) {

		super(registry, jobId, jobName, scopeFormat.formatScope(parent, jobId, jobName));

		this.parent = checkNotNull(parent);
	}

	public final JobManagerMetricGroup parent() {
		return parent;
	}

	@Override
	protected Iterable<? extends ComponentMetricGroup> subComponents() {
		return Collections.emptyList();
	}
}
