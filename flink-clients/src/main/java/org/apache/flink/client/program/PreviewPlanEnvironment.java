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

package org.apache.flink.client.program;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.dag.DataSinkNode;

import java.util.List;

/**
 * Environment to extract the pre-optimized plan.
 */
public final class PreviewPlanEnvironment extends ExecutionEnvironment {

	List<DataSinkNode> previewPlan;

	String preview;

	Plan plan;

	StreamPlanEnvironment streamPlanEnvironment;

	public PreviewPlanEnvironment() {
		this.streamPlanEnvironment = new StreamPlanEnvironment(this);
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		this.plan = createProgramPlan(jobName);
		this.previewPlan = Optimizer.createPreOptimizedPlan(plan);

		// do not go on with anything now!
		throw new OptimizerPlanEnvironment.ProgramAbortException();
	}

	@Override
	public String getExecutionPlan() throws Exception {
		Plan plan = createProgramPlan("unused");
		this.previewPlan = Optimizer.createPreOptimizedPlan(plan);

		// do not go on with anything now!
		throw new OptimizerPlanEnvironment.ProgramAbortException();
	}

	@Override
	public void startNewSession() {
	}

	public void setAsContext() {
		ExecutionEnvironmentFactory factory = new ExecutionEnvironmentFactory() {
			@Override
			public ExecutionEnvironment createExecutionEnvironment() {
				return PreviewPlanEnvironment.this;
			}
		};
		initializeContextEnvironment(factory);
		streamPlanEnvironment.setAsContext();
	}

	public void unsetAsContext() {
		resetContextEnvironment();
		streamPlanEnvironment.unsetAsContext();
	}

	public void setPreview(String preview) {
		this.preview = preview;
	}

	public Plan getPlan() {
		return plan;
	}
}
