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

package org.apache.flink.yarn;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.yarn.util.YarnTestUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;


/**
 * Test cases for the cancellation of Yarn Flink clusters.
 */
public class YARNJobCancellationITCase extends YarnTestBase {
	private static final Logger LOG = LoggerFactory.getLogger(YARNJobCancellationITCase.class);

	private final Duration yarnAppTerminateTimeout = Duration.ofSeconds(10);

	private final int sleepIntervalInMS = 100;

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@BeforeClass
	public static void setup() {
		YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-per-job-cancellation");
		startYARNWithConfig(YARN_CONFIGURATION);
	}

	@Test
	public void testPerCancellationWithInfiniteSource() throws Exception {
		runTest(() -> deployPerjob(getContinuouslyRunningTestingJobGraph()));
	}

	private void deployPerjob(JobGraph jobGraph) throws Exception {

		Configuration configuration = new Configuration();
		configuration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1g"));
		configuration.setString(AkkaOptions.ASK_TIMEOUT, "30 s");

		try (final YarnClusterDescriptor yarnClusterDescriptor = createYarnClusterDescriptor(configuration)) {

			yarnClusterDescriptor.setLocalJarPath(new Path(flinkUberjar.getAbsolutePath()));
			yarnClusterDescriptor.addShipFiles(Arrays.asList(flinkLibFolder.listFiles()));
			yarnClusterDescriptor.addShipFiles(Arrays.asList(flinkShadedHadoopDir.listFiles()));

			final ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
				.setMasterMemoryMB(768)
				.setTaskManagerMemoryMB(1024)
				.setSlotsPerTaskManager(1)
				.createClusterSpecification();

			File testingJar = YarnTestBase.findFile("..", new YarnTestUtils.TestJarFinder("flink-yarn-tests"));

			jobGraph.addJar(new org.apache.flink.core.fs.Path(testingJar.toURI()));
			try (ClusterClient<ApplicationId> clusterClient = yarnClusterDescriptor
				.deployJobCluster(
					clusterSpecification,
					jobGraph,
					false)
				.getClusterClient()) {

				ApplicationId applicationId = clusterClient.getClusterId();

				CompletableFuture<?> cancelResult = clusterClient.cancel(jobGraph.getJobID());
				cancelResult.get();

				waitApplicationFinishedElseKillIt(
					applicationId, yarnAppTerminateTimeout, yarnClusterDescriptor, sleepIntervalInMS);
			}
		}
	}

	private JobGraph getContinuouslyRunningTestingJobGraph() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		env.addSource(new InfiniteSource())
			.shuffle()
			.addSink(new DiscardingSink<>());

		return env.getStreamGraph().getJobGraph();
	}

	private static class InfiniteSource implements SourceFunction<String> {

		private volatile boolean running = true;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (running) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect("test");
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}

