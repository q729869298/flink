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

package org.apache.flink.test.classloading;

import java.io.File;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.testdata.KMeansData;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ClassLoaderITCase {

	private static final String INPUT_SPLITS_PROG_JAR_FILE = "target/customsplit-test-jar.jar";

	private static final String STREAMING_PROG_JAR_FILE = "target/streamingclassloader-test-jar.jar";

	private static final String STREAMING_CHECKPOINTED_PROG_JAR_FILE = "target/streaming-checkpointed-classloader-test-jar.jar";

	private static final String KMEANS_JAR_PATH = "target/kmeans-test-jar.jar";

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void testJobsWithCustomClassLoader() {
		try {

			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 2);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 2);
			config.setString(ConfigConstants.DEFAULT_EXECUTION_RETRY_DELAY_KEY, "0 s");

			// we need to use the "filesystem" state backend to ensure FLINK-2543 is not happening again.
			config.setString(ConfigConstants.STATE_BACKEND, "filesystem");
			config.setString(ConfigConstants.STATE_BACKEND_FS_DIR, "file://" + folder.newFolder().getAbsolutePath());

			ForkableFlinkMiniCluster testCluster = new ForkableFlinkMiniCluster(config, false);

			try {
				int port = testCluster.getJobManagerRPCPort();

				PackagedProgram inputSplitTestProg = new PackagedProgram(new File(INPUT_SPLITS_PROG_JAR_FILE),
						new String[] { INPUT_SPLITS_PROG_JAR_FILE,
										"localhost",
										String.valueOf(port),
										"4" // parallelism
									} );
				inputSplitTestProg.invokeInteractiveModeForExecution();

				// regular streaming job
				PackagedProgram streamingProg = new PackagedProgram(new File(STREAMING_PROG_JAR_FILE),
						new String[] { STREAMING_PROG_JAR_FILE, "localhost", String.valueOf(port) } );
				streamingProg.invokeInteractiveModeForExecution();

				// checkpointed streaming job with custom classes for the checkpoint (FLINK-2543)
				// the test also ensures that user specific exceptions are serializable between JobManager <--> JobClient.
				try {
					PackagedProgram streamingCheckpointedProg = new PackagedProgram(new File(STREAMING_CHECKPOINTED_PROG_JAR_FILE),
							new String[]{STREAMING_CHECKPOINTED_PROG_JAR_FILE, "localhost", String.valueOf(port)});
					streamingCheckpointedProg.invokeInteractiveModeForExecution();
				} catch(Exception e) {
					// we can not access the SuccessException here when executing the tests with maven, because its not available in the jar.
					try {
						if (!(e.getCause().getCause().getClass().getCanonicalName().equals("org.apache.flink.test.classloading.jar.CheckpointedStreamingProgram.SuccessException"))) {
							throw e;
						}
					} catch(Throwable ignore) {
						throw e;
					}
				}

				PackagedProgram kMeansProg = new PackagedProgram(new File(KMEANS_JAR_PATH),
						new String[] { KMEANS_JAR_PATH,
										"localhost",
										String.valueOf(port),
										"4", // parallelism
										KMeansData.DATAPOINTS,
										KMeansData.INITIAL_CENTERS,
										"25"
									} );
				kMeansProg.invokeInteractiveModeForExecution();
			}
			finally {
				testCluster.shutdown();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

}
