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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.util.FlinkException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CompletedCheckpointStore} for JobManagers running in {@link HighAvailabilityMode#ZOOKEEPER}.
 *
 * <p>Checkpoints are added under a ZNode per job:
 * <pre>
 * +----O /flink/checkpoints/&lt;job-id&gt;  [persistent]
 * .    |
 * .    +----O /flink/checkpoints/&lt;job-id&gt;/1 [persistent]
 * .    .                                  .
 * .    .                                  .
 * .    .                                  .
 * .    +----O /flink/checkpoints/&lt;job-id&gt;/N [persistent]
 * </pre>
 *
 * <p>During recovery, the latest checkpoint is read from ZooKeeper. If there is more than one,
 * only the latest one is used and older ones are discarded (even if the maximum number
 * of retained checkpoints is greater than one).
 *
 * <p>If there is a network partition and multiple JobManagers run concurrent checkpoints for the
 * same program, it is OK to take any valid successful checkpoint as long as the "history" of
 * checkpoints is consistent. Currently, after recovery we start out with only a single
 * checkpoint to circumvent those situations.
 */
public class ZooKeeperCompletedCheckpointStore implements CompletedCheckpointStore {

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperCompletedCheckpointStore.class);

	/** Curator ZooKeeper client. */
	private final CuratorFramework client;

	/** Completed checkpoints in ZooKeeper. */
	private final ZooKeeperStateHandleStore<CompletedCheckpoint> checkpointsInZooKeeper;

	/** The maximum number of checkpoints to retain (at least 1). */
	private final int maxNumberOfCheckpointsToRetain;

	/**
	 * Local copy of the completed checkpoints in ZooKeeper. This is restored from ZooKeeper
	 * when recovering and is maintained in parallel to the state in ZooKeeper during normal
	 * operations.
	 */
	private final ArrayDeque<Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String>> completedCheckpoints;

	/**
	 * Creates a {@link ZooKeeperCompletedCheckpointStore} instance.
	 *
	 * @param maxNumberOfCheckpointsToRetain The maximum number of checkpoints to retain (at
	 *                                       least 1). Adding more checkpoints than this results
	 *                                       in older checkpoints being discarded. On recovery,
	 *                                       we will only start with a single checkpoint.
	 * @param client                         The Curator ZooKeeper client
	 * @param checkpointsPath                The ZooKeeper path for the checkpoints (needs to
	 *                                       start with a '/')
	 * @param stateStorage                   State storage to be used to persist the completed
	 *                                       checkpoint
	 * @param executor to give to the ZooKeeperStateHandleStore to run ZooKeeper callbacks
	 * @throws Exception
	 */
	public ZooKeeperCompletedCheckpointStore(
			int maxNumberOfCheckpointsToRetain,
			CuratorFramework client,
			String checkpointsPath,
			RetrievableStateStorageHelper<CompletedCheckpoint> stateStorage,
			Executor executor) throws Exception {

		checkArgument(maxNumberOfCheckpointsToRetain >= 1, "Must retain at least one checkpoint.");
		checkNotNull(stateStorage, "State storage");

		this.maxNumberOfCheckpointsToRetain = maxNumberOfCheckpointsToRetain;

		checkNotNull(client, "Curator client");
		checkNotNull(checkpointsPath, "Checkpoints path");

		// Ensure that the checkpoints path exists
		client.newNamespaceAwareEnsurePath(checkpointsPath)
				.ensure(client.getZookeeperClient());

		// All operations will have the path as root
		this.client = client.usingNamespace(client.getNamespace() + checkpointsPath);

		this.checkpointsInZooKeeper = new ZooKeeperStateHandleStore<>(this.client, stateStorage, executor);

		this.completedCheckpoints = new ArrayDeque<>(maxNumberOfCheckpointsToRetain + 1);

		LOG.info("Initialized in '{}'.", checkpointsPath);
	}

	@Override
	public boolean requiresExternalizedCheckpoints() {
		return true;
	}

	/**
	 * Gets the latest checkpoint from ZooKeeper and removes all others.
	 *
	 * <p><strong>Important</strong>: Even if there are more than one checkpoint in ZooKeeper,
	 * this will only recover the latest and discard the others. Otherwise, there is no guarantee
	 * that the history of checkpoints is consistent.
	 */
	@Override
	public void recover() throws Exception {
		LOG.info("Recovering checkpoints from ZooKeeper.");

		// Clear local handles in order to prevent duplicates on
		// recovery. The local handles should reflect the state
		// of ZooKeeper.
		completedCheckpoints.clear();

		while (true) {
			try {
				completedCheckpoints.addAll(checkpointsInZooKeeper.getAllSortedByNameAndLock());
				break;
			}
			catch (ConcurrentModificationException e) {
				LOG.warn("Concurrent modification while reading from ZooKeeper. Retrying.");
			}
		}

		int numberOfInitialCheckpoints = completedCheckpoints.size();

		LOG.info("Found {} checkpoints in ZooKeeper.", numberOfInitialCheckpoints);
	}

	/**
	 * Synchronously writes the new checkpoints to ZooKeeper and asynchronously removes older ones.
	 *
	 * @param checkpoint Completed checkpoint to add.
	 */
	@Override
	public void addCheckpoint(final CompletedCheckpoint checkpoint) throws Exception {
		checkNotNull(checkpoint, "Checkpoint");

		final String path = checkpointIdToPath(checkpoint.getCheckpointID());

		// Now add the new one. If it fails, we don't want to loose existing data.
		RetrievableStateHandle<CompletedCheckpoint> serializedCheckpoint =
			checkpointsInZooKeeper.addAndLock(path, checkpoint);

		completedCheckpoints.addLast(new Tuple2<>(serializedCheckpoint, path));

		// Everything worked, let's remove a previous checkpoint if necessary.
		while (completedCheckpoints.size() > maxNumberOfCheckpointsToRetain) {
			try {
				removeSubsumed(completedCheckpoints.removeFirst());
			} catch (Exception e) {
				LOG.warn("Failed to subsume the old checkpoint", e);
			}
		}

		LOG.debug("Added {} to {}.", checkpoint, path);
	}

	@Override
	public CompletedCheckpoint getLatestCheckpoint() throws FlinkException {
		if (completedCheckpoints.isEmpty()) {
			return null;
		}
		else {

			Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> serializedCheckpoint =
				completedCheckpoints.peekLast();

			return retrieveCompletedCheckpoint(serializedCheckpoint);
		}
	}

	@Override
	public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
		List<CompletedCheckpoint> result = new ArrayList<>(completedCheckpoints.size());
		for (Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> serializedCheckpoint : completedCheckpoints) {
			result.add(retrieveCompletedCheckpoint(serializedCheckpoint));
		}
		return result;
	}

	@Override
	public int getNumberOfRetainedCheckpoints() {
		return completedCheckpoints.size();
	}

	@Override
	public int getMaxNumberOfRetainedCheckpoints() {
		return maxNumberOfCheckpointsToRetain;
	}

	@Override
	public void shutdown(JobStatus jobStatus) throws Exception {
		if (jobStatus.isGloballyTerminalState()) {
			LOG.info("Shutting down");

			for (Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> checkpoint : completedCheckpoints) {
				try {
					removeShutdown(checkpoint, jobStatus);
				} catch (Exception e) {
					LOG.error("Failed to discard checkpoint.", e);
				}
			}

			completedCheckpoints.clear();

			String path = "/" + client.getNamespace();

			LOG.info("Removing {} from ZooKeeper", path);
			ZKPaths.deleteChildren(client.getZookeeperClient().getZooKeeper(), path, true);
		} else {
			LOG.info("Suspending");

			// Clear the local handles, but don't remove any state
			completedCheckpoints.clear();

			// Release the state handle locks in ZooKeeper such that they can be deleted
			checkpointsInZooKeeper.releaseAll();
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Removes a subsumed checkpoint from ZooKeeper and drops the state.
	 */
	private void removeSubsumed(
		final Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> completedCheckpoint) throws Exception {

		if (completedCheckpoint == null) {
			return;
		}

		CompletedCheckpoint deserializedCheckpoint = retrieveCompletedCheckpoint(completedCheckpoint);

		ZooKeeperStateHandleStore.RemoveCallback<CompletedCheckpoint> action =
			new ZooKeeperStateHandleStore.RemoveCallback<CompletedCheckpoint>() {
				@Override
				public void apply(@Nullable RetrievableStateHandle<CompletedCheckpoint> value) throws FlinkException {
					if (value != null) {
						try {
							// to keep them in sync
							// it will have been removed from the cached lists of checkpoints by
							// the caller
							deserializedCheckpoint.discardOnSubsume();
						} catch (Exception e) {
							throw new FlinkException("Could not discard the completed checkpoint on subsume.", e);
						}
					}
				}
			};

		checkpointsInZooKeeper.releaseAndTryRemove(completedCheckpoint.f1, action);
	}

	/**
	 * Removes a checkpoint from ZooKeeper because of Job shutdown and drops the state.
	 */
	private void removeShutdown(
			final Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> completedCheckpoint,
			final JobStatus jobStatus) throws Exception {

		if (completedCheckpoint == null) {
			return;
		}

		CompletedCheckpoint deserializedCheckpoint = retrieveCompletedCheckpoint(completedCheckpoint);

		ZooKeeperStateHandleStore.RemoveCallback<CompletedCheckpoint> removeAction = new ZooKeeperStateHandleStore.RemoveCallback<CompletedCheckpoint>() {
			@Override
			public void apply(@Nullable RetrievableStateHandle<CompletedCheckpoint> value) throws FlinkException {
				try {
					deserializedCheckpoint.discardOnShutdown(jobStatus);
				} catch (Exception e) {
					throw new FlinkException("Could not discard the completed checkpoint on subsume.", e);
				}
			}
		};

		checkpointsInZooKeeper.releaseAndTryRemove(completedCheckpoint.f1, removeAction);
	}

	/**
	 * Convert a checkpoint id into a ZooKeeper path.
	 *
	 * @param checkpointId to convert to the path
	 * @return Path created from the given checkpoint id
	 */
	public static String checkpointIdToPath(long checkpointId) {
		return String.format("/%019d", checkpointId);
	}

	/**
	 * Converts a path to the checkpoint id.
	 *
	 * @param path in ZooKeeper
	 * @return Checkpoint id parsed from the path
	 */
	public static long pathToCheckpointId(String path) {
		try {
			String numberString;

			// check if we have a leading slash
			if ('/' == path.charAt(0) ) {
				numberString = path.substring(1);
			} else {
				numberString = path;
			}
			return Long.parseLong(numberString);
		} catch (NumberFormatException e) {
			LOG.warn("Could not parse checkpoint id from {}. This indicates that the " +
				"checkpoint id to path conversion has changed.", path);

			return -1L;
		}
	}

	private static CompletedCheckpoint retrieveCompletedCheckpoint(Tuple2<RetrievableStateHandle<CompletedCheckpoint>, String> stateHandlePath) throws FlinkException {
		long checkpointId = pathToCheckpointId(stateHandlePath.f1);

		LOG.info("Trying to retrieve checkpoint {}.", checkpointId);

		try {
			return stateHandlePath.f0.retrieveState();
		} catch (ClassNotFoundException cnfe) {
			throw new FlinkException("Could not retrieve checkpoint " + checkpointId + " from state handle under " +
				stateHandlePath.f1 + ". This indicates that you are trying to recover from state written by an " +
				"older Flink version which is not compatible. Try cleaning the state handle store.", cnfe);
		} catch (IOException ioe) {
			throw new FlinkException("Could not retrieve checkpoint " + checkpointId + " from state handle under " +
				stateHandlePath.f1 + ". This indicates that the retrieved state handle is broken. Try cleaning the " +
				"state handle store.", ioe);
		}
	}
}
