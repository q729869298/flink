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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.util.SerializableObject;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.hamcrest.MockitoHamcrest;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests concerning the restoring of state from a checkpoint to the task executions. */
public class CheckpointStateRestoreTest {

    private static final String TASK_MANAGER_LOCATION_INFO = "Unknown location";

    /** Tests that on restore the task state is reset for each stateful task. */
    @Test
    public void testSetState() {
        try {

            KeyGroupRange keyGroupRange = KeyGroupRange.of(0, 0);
            List<SerializableObject> testStates =
                    Collections.singletonList(new SerializableObject());
            final KeyedStateHandle serializedKeyGroupStates =
                    CheckpointCoordinatorTestingUtils.generateKeyGroupState(
                            keyGroupRange, testStates);

            final JobID jid = new JobID();
            final JobVertexID statefulId = new JobVertexID();
            final JobVertexID statelessId = new JobVertexID();

            Execution statefulExec1 = mockExecution();
            Execution statefulExec2 = mockExecution();
            Execution statefulExec3 = mockExecution();
            Execution statelessExec1 = mockExecution();
            Execution statelessExec2 = mockExecution();

            ExecutionVertex stateful1 = mockExecutionVertex(statefulExec1, statefulId, 0, 3);
            ExecutionVertex stateful2 = mockExecutionVertex(statefulExec2, statefulId, 1, 3);
            ExecutionVertex stateful3 = mockExecutionVertex(statefulExec3, statefulId, 2, 3);
            ExecutionVertex stateless1 = mockExecutionVertex(statelessExec1, statelessId, 0, 2);
            ExecutionVertex stateless2 = mockExecutionVertex(statelessExec2, statelessId, 1, 2);

            ExecutionJobVertex stateful =
                    mockExecutionJobVertex(
                            statefulId, new ExecutionVertex[] {stateful1, stateful2, stateful3});
            ExecutionJobVertex stateless =
                    mockExecutionJobVertex(
                            statelessId, new ExecutionVertex[] {stateless1, stateless2});

            Set<ExecutionJobVertex> tasks = new HashSet<>();
            tasks.add(stateful);
            tasks.add(stateless);

            ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor =
                    new ManuallyTriggeredScheduledExecutor();

            CheckpointCoordinator coord =
                    new CheckpointCoordinatorBuilder()
                            .setJobId(jid)
                            .setTasksToTrigger(
                                    new ExecutionVertex[] {
                                        stateful1, stateful2, stateful3, stateless1, stateless2
                                    })
                            .setTasksToWaitFor(
                                    new ExecutionVertex[] {
                                        stateful1, stateful2, stateful3, stateless1, stateless2
                                    })
                            .setTasksToCommitTo(new ExecutionVertex[0])
                            .setTimer(manuallyTriggeredScheduledExecutor)
                            .build();

            // create ourselves a checkpoint with state
            coord.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();

            PendingCheckpoint pending = coord.getPendingCheckpoints().values().iterator().next();
            final long checkpointId = pending.getCheckpointId();

            final TaskStateSnapshot subtaskStates = new TaskStateSnapshot();

            subtaskStates.putSubtaskStateByOperatorID(
                    OperatorID.fromJobVertexID(statefulId),
                    OperatorSubtaskState.builder()
                            .setManagedKeyedState(serializedKeyGroupStates)
                            .build());

            coord.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            jid,
                            statefulExec1.getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            subtaskStates),
                    TASK_MANAGER_LOCATION_INFO);
            coord.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            jid,
                            statefulExec2.getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            subtaskStates),
                    TASK_MANAGER_LOCATION_INFO);
            coord.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            jid,
                            statefulExec3.getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            subtaskStates),
                    TASK_MANAGER_LOCATION_INFO);
            coord.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(jid, statelessExec1.getAttemptId(), checkpointId),
                    TASK_MANAGER_LOCATION_INFO);
            coord.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(jid, statelessExec2.getAttemptId(), checkpointId),
                    TASK_MANAGER_LOCATION_INFO);

            assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());
            assertEquals(0, coord.getNumberOfPendingCheckpoints());

            // let the coordinator inject the state
            assertTrue(coord.restoreLatestCheckpointedStateToAll(tasks, false));

            // verify that each stateful vertex got the state

            BaseMatcher<JobManagerTaskRestore> matcher =
                    new BaseMatcher<JobManagerTaskRestore>() {
                        @Override
                        public boolean matches(Object o) {
                            if (o instanceof JobManagerTaskRestore) {
                                JobManagerTaskRestore taskRestore = (JobManagerTaskRestore) o;
                                return Objects.equals(
                                        taskRestore.getTaskStateSnapshot(), subtaskStates);
                            }
                            return false;
                        }

                        @Override
                        public void describeTo(Description description) {
                            description.appendValue(subtaskStates);
                        }
                    };

            verify(statefulExec1, times(1)).setInitialState(MockitoHamcrest.argThat(matcher));
            verify(statefulExec2, times(1)).setInitialState(MockitoHamcrest.argThat(matcher));
            verify(statefulExec3, times(1)).setInitialState(MockitoHamcrest.argThat(matcher));
            verify(statelessExec1, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
            verify(statelessExec2, times(0)).setInitialState(Mockito.<JobManagerTaskRestore>any());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testNoCheckpointAvailable() {
        try {
            CheckpointCoordinator coord = new CheckpointCoordinatorBuilder().build();

            final boolean restored =
                    coord.restoreLatestCheckpointedStateToAll(Collections.emptySet(), false);
            assertFalse(restored);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     * Tests that the allow non restored state flag is correctly handled.
     *
     * <p>The flag only applies for state that is part of the checkpoint.
     */
    @Test
    public void testNonRestoredState() throws Exception {
        // --- (1) Create tasks to restore checkpoint with ---
        JobVertexID jobVertexId1 = new JobVertexID();
        JobVertexID jobVertexId2 = new JobVertexID();

        OperatorID operatorId1 = OperatorID.fromJobVertexID(jobVertexId1);

        // 1st JobVertex
        ExecutionVertex vertex11 = mockExecutionVertex(mockExecution(), jobVertexId1, 0, 3);
        ExecutionVertex vertex12 = mockExecutionVertex(mockExecution(), jobVertexId1, 1, 3);
        ExecutionVertex vertex13 = mockExecutionVertex(mockExecution(), jobVertexId1, 2, 3);
        // 2nd JobVertex
        ExecutionVertex vertex21 = mockExecutionVertex(mockExecution(), jobVertexId2, 0, 2);
        ExecutionVertex vertex22 = mockExecutionVertex(mockExecution(), jobVertexId2, 1, 2);

        ExecutionJobVertex jobVertex1 =
                mockExecutionJobVertex(
                        jobVertexId1, new ExecutionVertex[] {vertex11, vertex12, vertex13});
        ExecutionJobVertex jobVertex2 =
                mockExecutionJobVertex(jobVertexId2, new ExecutionVertex[] {vertex21, vertex22});

        Set<ExecutionJobVertex> tasks = new HashSet<>();
        tasks.add(jobVertex1);
        tasks.add(jobVertex2);

        CheckpointCoordinator coord = new CheckpointCoordinatorBuilder().build();

        // --- (2) Checkpoint misses state for a jobVertex (should work) ---
        Map<OperatorID, OperatorState> checkpointTaskStates = new HashMap<>();
        {
            OperatorState taskState = new OperatorState(operatorId1, 3, 3);
            taskState.putState(0, OperatorSubtaskState.builder().build());
            taskState.putState(1, OperatorSubtaskState.builder().build());
            taskState.putState(2, OperatorSubtaskState.builder().build());

            checkpointTaskStates.put(operatorId1, taskState);
        }
        CompletedCheckpoint checkpoint =
                new CompletedCheckpoint(
                        new JobID(),
                        0,
                        1,
                        2,
                        new HashMap<>(checkpointTaskStates),
                        Collections.<MasterState>emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new TestCompletedCheckpointStorageLocation());

        coord.getCheckpointStore().addCheckpoint(checkpoint, new CheckpointsCleaner(), () -> {});

        assertTrue(coord.restoreLatestCheckpointedStateToAll(tasks, false));
        assertTrue(coord.restoreLatestCheckpointedStateToAll(tasks, true));

        // --- (3) JobVertex missing for task state that is part of the checkpoint ---
        JobVertexID newJobVertexID = new JobVertexID();
        OperatorID newOperatorID = OperatorID.fromJobVertexID(newJobVertexID);

        // There is no task for this
        {
            OperatorState taskState = new OperatorState(newOperatorID, 1, 1);
            taskState.putState(0, OperatorSubtaskState.builder().build());

            checkpointTaskStates.put(newOperatorID, taskState);
        }

        checkpoint =
                new CompletedCheckpoint(
                        new JobID(),
                        1,
                        2,
                        3,
                        new HashMap<>(checkpointTaskStates),
                        Collections.<MasterState>emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new TestCompletedCheckpointStorageLocation());

        coord.getCheckpointStore().addCheckpoint(checkpoint, new CheckpointsCleaner(), () -> {});

        // (i) Allow non restored state (should succeed)
        final boolean restored = coord.restoreLatestCheckpointedStateToAll(tasks, true);
        assertTrue(restored);

        // (ii) Don't allow non restored state (should fail)
        try {
            coord.restoreLatestCheckpointedStateToAll(tasks, false);
            fail("Did not throw the expected Exception.");
        } catch (IllegalStateException ignored) {
        }
    }

    // ------------------------------------------------------------------------

    private Execution mockExecution() {
        return mockExecution(ExecutionState.RUNNING);
    }

    private Execution mockExecution(ExecutionState state) {
        Execution mock = mock(Execution.class);
        when(mock.getAttemptId()).thenReturn(new ExecutionAttemptID());
        when(mock.getState()).thenReturn(state);
        return mock;
    }

    private ExecutionVertex mockExecutionVertex(
            Execution execution, JobVertexID vertexId, int subtask, int parallelism) {
        ExecutionVertex mock = mock(ExecutionVertex.class);
        when(mock.getJobvertexId()).thenReturn(vertexId);
        when(mock.getParallelSubtaskIndex()).thenReturn(subtask);
        when(mock.getCurrentExecutionAttempt()).thenReturn(execution);
        when(mock.getTotalNumberOfParallelSubtasks()).thenReturn(parallelism);
        when(mock.getMaxParallelism()).thenReturn(parallelism);
        return mock;
    }

    private ExecutionJobVertex mockExecutionJobVertex(JobVertexID id, ExecutionVertex[] vertices) {
        ExecutionJobVertex vertex = mock(ExecutionJobVertex.class);
        when(vertex.getParallelism()).thenReturn(vertices.length);
        when(vertex.getMaxParallelism()).thenReturn(vertices.length);
        when(vertex.getJobVertexId()).thenReturn(id);
        when(vertex.getTaskVertices()).thenReturn(vertices);
        when(vertex.getOperatorIDs())
                .thenReturn(
                        Collections.singletonList(
                                OperatorIDPair.generatedIDOnly(OperatorID.fromJobVertexID(id))));
        when(vertex.getProducedDataSets()).thenReturn(new IntermediateResult[0]);

        for (ExecutionVertex v : vertices) {
            when(v.getJobVertex()).thenReturn(vertex);
        }
        return vertex;
    }
}
