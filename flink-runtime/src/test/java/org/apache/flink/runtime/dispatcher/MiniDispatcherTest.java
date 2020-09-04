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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerResource;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link MiniDispatcher}.
 */
public class MiniDispatcherTest extends TestLogger {

	private static final Time timeout = Time.seconds(10L);

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final TestingFatalErrorHandlerResource testingFatalErrorHandlerResource = new TestingFatalErrorHandlerResource();

	private static JobGraph jobGraph;

	private static ArchivedExecutionGraph archivedExecutionGraph;

	private static TestingRpcService rpcService;

	private static Configuration configuration;

	private static BlobServer blobServer;

	private final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();

	private final HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, 1000L);

	private final ArchivedExecutionGraphStore archivedExecutionGraphStore = new MemoryArchivedExecutionGraphStore();

	private TestingHighAvailabilityServices highAvailabilityServices;

	private TestingJobManagerRunnerFactory testingJobManagerRunnerFactory;

	@BeforeClass
	public static void setupClass() throws IOException {
		jobGraph = new JobGraph();

		archivedExecutionGraph = new ArchivedExecutionGraphBuilder()
			.setJobID(jobGraph.getJobID())
			.setState(JobStatus.FINISHED)
			.build();

		rpcService = new TestingRpcService();
		configuration = new Configuration();

		configuration.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		blobServer = new BlobServer(configuration, new VoidBlobStore());
	}

	@Before
	public void setup() throws Exception {
		highAvailabilityServices = new TestingHighAvailabilityServicesBuilder().build();

		testingJobManagerRunnerFactory = new TestingJobManagerRunnerFactory();
	}

	@AfterClass
	public static void teardownClass() throws IOException, InterruptedException, ExecutionException, TimeoutException {
		if (blobServer != null) {
			blobServer.close();
		}

		if (rpcService != null) {
			RpcUtils.terminateRpcService(rpcService, timeout);
		}
	}

	/**
	 * Tests that the {@link MiniDispatcher} recovers the single job with which it
	 * was started.
	 */
	@Test
	public void testSingleJobRecovery() throws Exception {
		final MiniDispatcher miniDispatcher = createMiniDispatcher(ClusterEntrypoint.ExecutionMode.DETACHED);

		miniDispatcher.start();

		try {
			final TestingJobManagerRunner testingJobManagerRunner = testingJobManagerRunnerFactory.takeCreatedJobManagerRunner();

			assertThat(testingJobManagerRunner.getJobID(), is(jobGraph.getJobID()));
		} finally {
			RpcUtils.terminateRpcEndpoint(miniDispatcher, timeout);
		}
	}

	/**
	 * Tests that in detached mode, the {@link MiniDispatcher} will complete the future that
	 * signals job termination.
	 */
	@Test
	public void testTerminationAfterJobCompletion() throws Exception {
		final MiniDispatcher miniDispatcher = createMiniDispatcher(ClusterEntrypoint.ExecutionMode.DETACHED);

		miniDispatcher.start();

		try {
			// wait until we have submitted the job
			final TestingJobManagerRunner testingJobManagerRunner = testingJobManagerRunnerFactory.takeCreatedJobManagerRunner();

			testingJobManagerRunner.completeResultFuture(archivedExecutionGraph);

			// wait until we terminate
			miniDispatcher.getShutDownFuture().get();
		} finally {
			RpcUtils.terminateRpcEndpoint(miniDispatcher, timeout);
		}
	}

	/**
	 * Tests that the {@link MiniDispatcher} only terminates in {@link ClusterEntrypoint.ExecutionMode#NORMAL}
	 * after it has served the {@link org.apache.flink.runtime.jobmaster.JobResult} once.
	 */
	@Test
	public void testJobResultRetrieval() throws Exception {
		final MiniDispatcher miniDispatcher = createMiniDispatcher(ClusterEntrypoint.ExecutionMode.NORMAL);

		miniDispatcher.start();

		try {
			// wait until we have submitted the job
			final TestingJobManagerRunner testingJobManagerRunner = testingJobManagerRunnerFactory.takeCreatedJobManagerRunner();

			testingJobManagerRunner.completeResultFuture(archivedExecutionGraph);

			assertFalse(miniDispatcher.getTerminationFuture().isDone());

			final DispatcherGateway dispatcherGateway = miniDispatcher.getSelfGateway(DispatcherGateway.class);

			final CompletableFuture<JobResult> jobResultFuture = dispatcherGateway.requestJobResult(jobGraph.getJobID(), timeout);

			final JobResult jobResult = jobResultFuture.get();

			assertThat(jobResult.getJobId(), is(jobGraph.getJobID()));
		}
		finally {
			RpcUtils.terminateRpcEndpoint(miniDispatcher, timeout);
		}
	}

	@Test
	public void testShutdownIfJobCancelledInNormalMode() throws Exception {
		final MiniDispatcher miniDispatcher = createMiniDispatcher(ClusterEntrypoint.ExecutionMode.NORMAL);
		miniDispatcher.start();

		try {
			// wait until we have submitted the job
			final TestingJobManagerRunner testingJobManagerRunner = testingJobManagerRunnerFactory.takeCreatedJobManagerRunner();

			assertFalse(miniDispatcher.getTerminationFuture().isDone());

			final DispatcherGateway dispatcherGateway = miniDispatcher.getSelfGateway(DispatcherGateway.class);

			dispatcherGateway.cancelJob(jobGraph.getJobID(), Time.seconds(10L));
			testingJobManagerRunner.completeResultFuture(new ArchivedExecutionGraphBuilder()
				.setJobID(jobGraph.getJobID())
				.setState(JobStatus.CANCELED)
				.build());

			ApplicationStatus applicationStatus = miniDispatcher.getShutDownFuture().get();
			assertThat(applicationStatus, is(ApplicationStatus.CANCELED));
		}
		finally {
			RpcUtils.terminateRpcEndpoint(miniDispatcher, timeout);
		}
	}

	// --------------------------------------------------------
	// Utilities
	// --------------------------------------------------------

	@Nonnull
	private MiniDispatcher createMiniDispatcher(ClusterEntrypoint.ExecutionMode executionMode) throws Exception {
		return new MiniDispatcher(
			rpcService,
			DispatcherId.generate(),
			new DispatcherServices(
				configuration,
				highAvailabilityServices,
				() -> CompletableFuture.completedFuture(resourceManagerGateway),
				blobServer,
				heartbeatServices,
				archivedExecutionGraphStore,
				testingFatalErrorHandlerResource.getFatalErrorHandler(),
				VoidHistoryServerArchivist.INSTANCE,
				null,
				UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
				highAvailabilityServices.getJobGraphStore(),
				testingJobManagerRunnerFactory),
			new DefaultDispatcherBootstrap(Collections.singletonList(jobGraph)),
			executionMode);
	}

}
