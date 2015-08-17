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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.SerializedJobExecutionResult;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class ClusterUtil {
	private static final Logger LOG = LoggerFactory.getLogger(ClusterUtil.class);

	private static LocalFlinkMiniCluster exec = null;

	/**
	 * Executes the given JobGraph locally, on a FlinkMiniCluster
	 * 
	 * @param jobGraph
	 *            jobGraph
	 * @param parallelism
	 *            numberOfTaskTrackers
	 * @param memorySize
	 *            memorySize
	 * @param printDuringExecution
	 * @param detached
	 * @param customConf
	 * 		Custom configuration for the LocalExecutor. Can be null.
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 */
	public static JobExecutionResult runOnMiniCluster(JobGraph jobGraph, int parallelism, long memorySize,
													boolean printDuringExecution, boolean detached, Configuration customConf)
			throws Exception {

		Configuration configuration = jobGraph.getJobConfiguration();

		LocalFlinkMiniCluster exec = null;

		configuration.setLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, memorySize);
		configuration.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, parallelism);
		if(customConf != null) {
			configuration.addAll(customConf);
		}
		if (LOG.isInfoEnabled()) {
			LOG.info("Running on mini cluster");
		}

		try {
			exec = new LocalFlinkMiniCluster(configuration, true);
			if(detached) {
				exec.submitJobDetached(jobGraph);
				return null;
			} else {
				SerializedJobExecutionResult result = exec.submitJobAndWait(jobGraph, printDuringExecution);
				return result.toJobExecutionResult(ClusterUtil.class.getClassLoader());
			}
		} finally {
			if (exec != null && !detached) {
				exec.stop();
			}
		}
	}

	/**
	 * Start a job in a detached mode on a local mini cluster.
	 */
	public static void startOnMiniCluster(JobGraph jobGraph, int parallelism, long memorySize) throws Exception {
		runOnMiniCluster(jobGraph, parallelism, memorySize, true, true, null);
	}

	public static void stopOnMiniCluster() {
		if(exec != null) {
			exec.stop();
			exec = null;
		} else {
			throw new IllegalStateException("Cluster was not started via .start(...)");
		}
	}

}
