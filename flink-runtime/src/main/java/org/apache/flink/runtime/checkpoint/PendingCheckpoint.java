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
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV1;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.CheckpointMetadataStreamFactory;
import org.apache.flink.runtime.state.CheckpointMetadataStreamFactory.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointMetadataStreamFactory.StreamHandleAndPointer;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pending checkpoint is a checkpoint that has been started, but has not been
 * acknowledged by all tasks that need to acknowledge it. Once all tasks have
 * acknowledged it, it becomes a {@link CompletedCheckpoint}.
 * 
 * <p>Note that the pending checkpoint, as well as the successful checkpoint keep the
 * state handles always as serialized values, never as actual values.
 */
public class PendingCheckpoint {

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

	private final Object lock = new Object();

	private final JobID jobId;

	private final long checkpointId;

	private final long checkpointTimestamp;

	private final Map<JobVertexID, TaskState> taskStates;

	private final Map<ExecutionAttemptID, ExecutionVertex> notYetAcknowledgedTasks;

	/** Set of acknowledged tasks */
	private final Set<ExecutionAttemptID> acknowledgedTasks;

	/** The checkpoint properties. If the checkpoint should be persisted
	 * externally, it happens in {@link #finalizeCheckpointExternalized()}. */
	private final CheckpointProperties props;

	/** The promise to fulfill once the checkpoint has been completed. */
	private final FlinkCompletableFuture<CompletedCheckpoint> onCompletionPromise;

	/** The executor for potentially blocking I/O operations, like state disposal */
	private final Executor executor;

	/** The stream factory to be used when persisting the metadata.
	 * Null, if the metadata is not intended to be persisted externally */
	@Nullable
	private final CheckpointMetadataStreamFactory metadataWriter;

	private int numAcknowledgedTasks;

	private boolean discarded;

	/** Optional stats tracker callback. */
	@Nullable
	private PendingCheckpointStats statsCallback;

	// --------------------------------------------------------------------------------------------

	public PendingCheckpoint(
			JobID jobId,
			long checkpointId,
			long checkpointTimestamp,
			Map<ExecutionAttemptID, ExecutionVertex> verticesToConfirm,
			CheckpointProperties props,
			Executor executor,
			@Nullable CheckpointMetadataStreamFactory metadataWriter) {

		// Sanity check
		checkArgument(metadataWriter != null || !(props.externalizeCheckpoint() || props.isSavepoint()),
					"Checkpoint metadata should be externalized, but metadata writer is null");

		checkArgument(verticesToConfirm.size() > 0,
				"Checkpoint needs at least one vertex that commits the checkpoint");

		this.jobId = checkNotNull(jobId);
		this.checkpointId = checkpointId;
		this.checkpointTimestamp = checkpointTimestamp;
		this.notYetAcknowledgedTasks = checkNotNull(verticesToConfirm);
		this.props = checkNotNull(props);
		this.executor = Preconditions.checkNotNull(executor);
		this.metadataWriter = metadataWriter;

		this.taskStates = new HashMap<>();
		this.acknowledgedTasks = new HashSet<>(verticesToConfirm.size());
		this.onCompletionPromise = new FlinkCompletableFuture<>();
	}

	// --------------------------------------------------------------------------------------------

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	public JobID getJobId() {
		return jobId;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	public long getCheckpointTimestamp() {
		return checkpointTimestamp;
	}

	public int getNumberOfNonAcknowledgedTasks() {
		return notYetAcknowledgedTasks.size();
	}

	public int getNumberOfAcknowledgedTasks() {
		return numAcknowledgedTasks;
	}

	public Map<JobVertexID, TaskState> getTaskStates() {
		return taskStates;
	}

	public boolean isFullyAcknowledged() {
		return this.notYetAcknowledgedTasks.isEmpty() && !discarded;
	}

	public boolean isDiscarded() {
		return discarded;
	}

	/**
	 * Checks whether this checkpoint can be subsumed or whether it should always continue, regardless
	 * of newer checkpoints in progress.
	 * 
	 * @return True if the checkpoint can be subsumed, false otherwise.
	 */
	public boolean canBeSubsumed() {
		// If the checkpoint is forced, it cannot be subsumed.
		return !props.forceCheckpoint();
	}

	public CheckpointProperties getProps() {
		return props;
	}

	/**
	 * Sets the callback for tracking this pending checkpoint.
	 *
	 * @param trackerCallback Callback for collecting subtask stats.
	 */
	void setStatsCallback(@Nullable PendingCheckpointStats trackerCallback) {
		this.statsCallback = trackerCallback;
	}

	// ------------------------------------------------------------------------
	//  Progress and Completion
	// ------------------------------------------------------------------------

	/**
	 * Returns the completion future.
	 *
	 * @return A future to the completed checkpoint
	 */
	public Future<CompletedCheckpoint> getCompletionFuture() {
		return onCompletionPromise;
	}

	/**
	 * Finalizes the pending checkpoint to a {@code CompletedCheckpoint} and persisting the checkpoint
	 * metadata in the process.
	 * 
	 * @return The completed checkpoint with externalized metadata.
	 * 
	 * @throws IOException Thrown, if the writing of the metadata failed due to an I/O error. 
	 * @throws IllegalStateException Thrown, if the pending checkpoint is not fully acknowledged.
	 */
	public CompletedCheckpoint finalizeCheckpointExternalized() throws IOException {
		checkState(metadataWriter != null, "No metadata writer available");
		
		synchronized (lock) {
			checkState(!discarded, "checkpoint is discarded");
			checkState(isFullyAcknowledged(), "Pending checkpoint has not been fully acknowledged yet.");

			// externalize the metadata
			try(CheckpointMetadataOutputStream out = metadataWriter.createCheckpointStateOutputStream();
				DataOutputStream dos = new DataOutputStream(out))
			{
				final Savepoint savepoint = new SavepointV1(checkpointId, taskStates.values());

				Checkpoints.storeCheckpointMetadata(savepoint, dos);
				dos.flush();
				final StreamHandleAndPointer result = out.closeAndGetPointerHandle();

				return finalizeInternal(result.stateHandle(), result.pointer());
			}
			catch (Throwable t) {
				// make sure we fulfill the promise with an exception if something fails
				onCompletionPromise.completeExceptionally(t);
				ExceptionUtils.rethrowIOException(t);
				return null; // silence the compiler
			}
		}
	}

	public CompletedCheckpoint finalizeCheckpointNonExternalized() {
		synchronized (lock) {
			checkState(!discarded, "checkpoint is discarded");
			checkState(isFullyAcknowledged(), "Pending checkpoint has not been fully acknowledged yet.");

			// make sure we fulfill the promise with an exception if something fails
			try {
				// finalize without external metadata
				return finalizeInternal(null, null);
			}
			catch (Throwable t) {
				onCompletionPromise.completeExceptionally(t);
				ExceptionUtils.rethrow(t);
				return null; // silence the compiler
			}
		}
	}

	@GuardedBy("lock")
	private CompletedCheckpoint finalizeInternal(
			@Nullable StreamStateHandle externalMetadata,
			@Nullable String externalPointer) {

		assert(Thread.holdsLock(lock));

		CompletedCheckpoint completed = new CompletedCheckpoint(
				jobId,
				checkpointId,
				checkpointTimestamp,
				System.currentTimeMillis(),
				new HashMap<>(taskStates),
				props,
				externalMetadata,
				externalPointer);

		onCompletionPromise.complete(completed);

		// to prevent null-pointers from concurrent modification, copy reference onto stack
		PendingCheckpointStats statsCallback = this.statsCallback;
		if (statsCallback != null) {
			// Finalize the statsCallback and give the completed checkpoint a
			// callback for discards.
			CompletedCheckpointStats.DiscardCallback discardCallback = 
					statsCallback.reportCompletedCheckpoint(externalPointer);
			completed.setDiscardCallback(discardCallback);
		}

		// mark this pending checkpoint as disposed, but do NOT drop the state
		dispose(false);

		return completed;
	}

	/**
	 * Acknowledges the task with the given execution attempt id and the given subtask state.
	 *
	 * @param executionAttemptId of the acknowledged task
	 * @param subtaskState of the acknowledged task
	 * @param metrics Checkpoint metrics for the stats
	 * @return TaskAcknowledgeResult of the operation
	 */
	public TaskAcknowledgeResult acknowledgeTask(
			ExecutionAttemptID executionAttemptId,
			SubtaskState subtaskState,
			CheckpointMetrics metrics) {

		synchronized (lock) {
			if (discarded) {
				return TaskAcknowledgeResult.DISCARDED;
			}

			final ExecutionVertex vertex = notYetAcknowledgedTasks.remove(executionAttemptId);

			if (vertex == null) {
				if (acknowledgedTasks.contains(executionAttemptId)) {
					return TaskAcknowledgeResult.DUPLICATE;
				} else {
					return TaskAcknowledgeResult.UNKNOWN;
				}
			} else {
				acknowledgedTasks.add(executionAttemptId);
			}

			JobVertexID jobVertexID = vertex.getJobvertexId();
			int subtaskIndex = vertex.getParallelSubtaskIndex();
			long ackTimestamp = System.currentTimeMillis();

			long stateSize = 0;
			if (null != subtaskState) {
				TaskState taskState = taskStates.get(jobVertexID);

				if (null == taskState) {
					@SuppressWarnings("deprecation")
					ChainedStateHandle<StreamStateHandle> nonPartitionedState = 
							subtaskState.getLegacyOperatorState();
					ChainedStateHandle<OperatorStateHandle> partitioneableState =
							subtaskState.getManagedOperatorState();
					//TODO this should go away when we remove chained state, assigning state to operators directly instead
					int chainLength;
					if (nonPartitionedState != null) {
						chainLength = nonPartitionedState.getLength();
					} else if (partitioneableState != null) {
						chainLength = partitioneableState.getLength();
					} else {
						chainLength = 1;
					}

					taskState = new TaskState(
							jobVertexID,
							vertex.getTotalNumberOfParallelSubtasks(),
							vertex.getMaxParallelism(),
							chainLength);

					taskStates.put(jobVertexID, taskState);
				}

				taskState.putState(subtaskIndex, subtaskState);
				stateSize = subtaskState.getStateSize();
			}

			++numAcknowledgedTasks;

			// publish the checkpoint statistics
			// to prevent null-pointers from concurrent modification, copy reference onto stack
			final PendingCheckpointStats statsCallback = this.statsCallback;
			if (statsCallback != null) {
				// Do this in millis because the web frontend works with them
				long alignmentDurationMillis = metrics.getAlignmentDurationNanos() / 1_000_000;

				SubtaskStateStats subtaskStateStats = new SubtaskStateStats(
					subtaskIndex,
					ackTimestamp,
					stateSize,
					metrics.getSyncDurationMillis(),
					metrics.getAsyncDurationMillis(),
					metrics.getBytesBufferedInAlignment(),
					alignmentDurationMillis);

				statsCallback.reportSubtaskStats(jobVertexID, subtaskStateStats);
			}

			return TaskAcknowledgeResult.SUCCESS;
		}
	}

	/**
	 * Result of the {@link PendingCheckpoint#acknowledgedTasks} method.
	 */
	public enum TaskAcknowledgeResult {
		SUCCESS, // successful acknowledge of the task
		DUPLICATE, // acknowledge message is a duplicate
		UNKNOWN, // unknown task acknowledged
		DISCARDED // pending checkpoint has been discarded
	}

	// ------------------------------------------------------------------------
	//  Cancellation
	// ------------------------------------------------------------------------

	/**
	 * Aborts a checkpoint because it expired (took too long).
	 */
	public void abortExpired() {
		try {
			Exception cause = new Exception("Checkpoint expired before completing");
			onCompletionPromise.completeExceptionally(cause);
			reportFailedCheckpoint(cause);
		} finally {
			dispose(true);
		}
	}

	/**
	 * Aborts the pending checkpoint because a newer completed checkpoint subsumed it.
	 */
	public void abortSubsumed() {
		try {
			Exception cause = new Exception("Checkpoints has been subsumed");
			onCompletionPromise.completeExceptionally(cause);
			reportFailedCheckpoint(cause);

			if (props.forceCheckpoint()) {
				throw new IllegalStateException("Bug: forced checkpoints must never be subsumed");
			}
		} finally {
			dispose(true);
		}
	}

	public void abortDeclined() {
		try {
			Exception cause = new Exception("Checkpoint was declined (tasks not ready)");
			onCompletionPromise.completeExceptionally(cause);
			reportFailedCheckpoint(cause);
		} finally {
			dispose(true);
		}
	}

	/**
	 * Aborts the pending checkpoint due to an error.
	 * @param cause The error's exception.
	 */
	public void abortError(Throwable cause) {
		try {
			Exception failure = new Exception("Checkpoint failed: " + cause.getMessage(), cause);
			onCompletionPromise.completeExceptionally(failure);
			reportFailedCheckpoint(failure);
		} finally {
			dispose(true);
		}
	}

	private void dispose(boolean releaseState) {
		synchronized (lock) {
			try {
				numAcknowledgedTasks = -1;
				if (!discarded && releaseState) {
					executor.execute(new Runnable() {
						@Override
						public void run() {
							try {
								StateUtil.bestEffortDiscardAllStateObjects(taskStates.values());
							} catch (Throwable t) {
								LOG.warn("Could not properly dispose the pending checkpoint {} of job {}.", 
										checkpointId, jobId, t);
							} finally {
								taskStates.clear();
							}
						}
					});

				}
			} finally {
				discarded = true;
				notYetAcknowledgedTasks.clear();
				acknowledgedTasks.clear();
			}
		}
	}

	/**
	 * Reports a failed checkpoint with the given optional cause.
	 *
	 * @param cause The failure cause or <code>null</code>.
	 */
	private void reportFailedCheckpoint(Exception cause) {
		// to prevent null-pointers from concurrent modification, copy reference onto stack
		final PendingCheckpointStats statsCallback = this.statsCallback;
		if (statsCallback != null) {
			long failureTimestamp = System.currentTimeMillis();
			statsCallback.reportFailedCheckpoint(failureTimestamp, cause);
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("Pending Checkpoint %d @ %d - confirmed=%d, pending=%d",
				checkpointId, checkpointTimestamp, getNumberOfAcknowledgedTasks(), getNumberOfNonAcknowledgedTasks());
	}
}
