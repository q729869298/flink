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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

/**
 * Tests for the failing of pending slot requests at the {@link SlotPool}.
 */
public class SlotPoolPendingRequestFailureTest extends TestLogger {

	private static final JobID jobId = new JobID();

	private static final ComponentMainThreadExecutor mainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forMainThread();
	public static final Time TIMEOUT = Time.seconds(10L);

	private TestingResourceManagerGateway resourceManagerGateway;

	@Before
	public void setup() {
		resourceManagerGateway = new TestingResourceManagerGateway();
	}

	/**
	 * Tests that failing an allocation fails the pending slot request.
	 */
	@Test
	public void testFailingAllocationFailsPendingSlotRequests() throws Exception {
		final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();
		resourceManagerGateway.setRequestSlotConsumer(slotRequest -> allocationIdFuture.complete(slotRequest.getAllocationId()));

		try (SlotPoolImpl slotPool = setupSlotPool()) {

			final CompletableFuture<PhysicalSlot> slotFuture = requestNewAllocatedSlot(slotPool, new SlotRequestId());

			final AllocationID allocationId = allocationIdFuture.get();

			assertThat(slotFuture.isDone(), is(false));

			final FlinkException cause = new FlinkException("Fail pending slot request failure.");
			final Optional<ResourceID> responseFuture = slotPool.failAllocation(allocationId, cause);

			assertThat(responseFuture.isPresent(), is(false));

			try {
				slotFuture.get();
				fail("Expected a slot allocation failure.");
			} catch (ExecutionException ee) {
				assertThat(ExceptionUtils.stripExecutionException(ee), equalTo(cause));
			}
		}
	}

	private CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(SlotPoolImpl slotPool, SlotRequestId slotRequestId) {
		return slotPool.requestNewAllocatedSlot(slotRequestId, ResourceProfile.UNKNOWN, TIMEOUT);
	}

	private SlotPoolImpl setupSlotPool() throws Exception {
		final SlotPoolImpl slotPool = new SlotPoolImpl(jobId);
		slotPool.start(JobMasterId.generate(), "foobar", mainThreadExecutor);
		slotPool.connectToResourceManager(resourceManagerGateway);

		return slotPool;
	}

}
