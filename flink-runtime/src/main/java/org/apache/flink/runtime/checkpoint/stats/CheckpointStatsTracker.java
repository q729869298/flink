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

package org.apache.flink.runtime.checkpoint.stats;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import scala.Option;

/**
 * A tracker for checkpoint statistics.
 *
 * <p>You can disable statistics by setting {@link ConfigConstants#JOB_MANAGER_WEB_CHECKPOINTS_DISABLE}.
 */
public interface CheckpointStatsTracker {

	/**
	 * Callback on a completed checkpoint.
	 *
	 * @param checkpoint The completed checkpoint.
	 */
	void onCompletedCheckpoint(CompletedCheckpoint checkpoint);

	/**
	 * Returns a snapshot of the checkpoint statistics for a job.
	 *
	 * @return Checkpoints stats for the job.
	 */
	Option<JobCheckpointStats> getJobStats();

	/**
	 * Returns a snapshot of the checkpoint statistics for an operator.
	 *
	 * @param operatorId The operator ID to gather the stats for.
	 *
	 * @return Checkpoint stats for the operator.
	 *
	 * @throws IllegalArgumentException If unknown operator ID.
	 */
	Option<OperatorCheckpointStats> getOperatorStats(JobVertexID operatorId);

}
