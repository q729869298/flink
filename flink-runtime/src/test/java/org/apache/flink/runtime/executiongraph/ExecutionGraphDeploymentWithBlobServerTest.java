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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Tests {@link ExecutionGraph} deployment when offloading job and task information into the BLOB
 * server.
 */
public class ExecutionGraphDeploymentWithBlobServerTest extends ExecutionGraphDeploymentTest {

	@Before
	public void setupBlobServer() throws IOException {
		Configuration config = new Configuration();
		// always offload the serialized job and task information
		config.setInteger(AkkaOptions.AKKA_RPC_OFFLOAD_MINSIZE, 0);
		blobServer = new BlobServer(config);
	}

	@After
	public void shutdownBlobServer() {
		if (blobServer != null) {
			blobServer.shutdown();
		}
	}

	@Override
	protected void checkJobOffloaded(ExecutionGraph eg) throws Exception {
		assertTrue(eg.hasJobInformationAtBlobStore());

		final String fileKey = ExecutionGraph.getOffloadedJobInfoFileName();
		// must not throw:
		blobServer.getURL(eg.getJobID(), fileKey);
	}

	@Override
	protected void checkTaskOffloaded(ExecutionGraph eg, JobVertexID jobVertexId) throws Exception {
		assertTrue(eg.getJobVertex(jobVertexId).hasTaskInformationAtBlobStore());

		final String fileKey = ExecutionJobVertex.getOffloadedTaskInfoFileName(jobVertexId);
		// must not throw:
		blobServer.getURL(eg.getJobID(), fileKey);
	}

	// TODO: test cleanup with failures?
}
