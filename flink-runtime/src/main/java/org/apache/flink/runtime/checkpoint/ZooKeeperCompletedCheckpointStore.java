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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.utils.ZKPaths;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobmanager.RecoveryMode;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.zookeeper.StateStorageHelper;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CompletedCheckpointStore} for JobManagers running in {@link RecoveryMode#ZOOKEEPER}.
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

	/** Curator ZooKeeper client */
	private final CuratorFramework client;

	/** Completed checkpoints in ZooKeeper */
	private final ZooKeeperStateHandleStore<CompletedCheckpoint> checkpointsInZooKeeper;

	/** The maximum number of checkpoints to retain (at least 1). */
	private final int maxNumberOfCheckpointsToRetain;

	/** User class loader for discarding {@link CompletedCheckpoint} instances. */
	private final ClassLoader userClassLoader;

	/** Local completed checkpoints. */
	private final ArrayDeque<Tuple2<StateHandle<CompletedCheckpoint>, String>> checkpointStateHandles;

	/**
	 * Creates a {@link ZooKeeperCompletedCheckpointStore} instance.
	 *
	 * @param maxNumberOfCheckpointsToRetain The maximum number of checkpoints to retain (at
	 *                                       least 1). Adding more checkpoints than this results
	 *                                       in older checkpoints being discarded. On recovery,
	 *                                       we will only start with a single checkpoint.
	 * @param userClassLoader                The user class loader used to discard checkpoints
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
			ClassLoader userClassLoader,
			CuratorFramework client,
			String checkpointsPath,
			StateStorageHelper<CompletedCheckpoint> stateStorage,
			Executor executor) throws Exception {

		checkArgument(maxNumberOfCheckpointsToRetain >= 1, "Must retain at least one checkpoint.");
		checkNotNull(stateStorage, "State storage");

		this.maxNumberOfCheckpointsToRetain = maxNumberOfCheckpointsToRetain;
		this.userClassLoader = checkNotNull(userClassLoader, "User class loader");

		checkNotNull(client, "Curator client");
		checkNotNull(checkpointsPath, "Checkpoints path");

		// Ensure that the checkpoints path exists
		client.newNamespaceAwareEnsurePath(checkpointsPath)
				.ensure(client.getZookeeperClient());

		// All operations will have the path as root
		this.client = client.usingNamespace(client.getNamespace() + checkpointsPath);

		this.checkpointsInZooKeeper = new ZooKeeperStateHandleStore<>(this.client, stateStorage, executor);

		this.checkpointStateHandles = new ArrayDeque<>(maxNumberOfCheckpointsToRetain + 1);

		LOG.info("Initialized in '{}'.", checkpointsPath);
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
		checkpointStateHandles.clear();

		// Get all there is first
		List<Tuple2<StateHandle<CompletedCheckpoint>, String>> initialCheckpoints;
		while (true) {
			try {
				initialCheckpoints = checkpointsInZooKeeper.getAllSortedByName();
				break;
			}
			catch (ConcurrentModificationException e) {
				LOG.warn("Concurrent modification while reading from ZooKeeper. Retrying.");
			}
		}

		int numberOfInitialCheckpoints = initialCheckpoints.size();

		LOG.info("Found {} checkpoints in ZooKeeper.", numberOfInitialCheckpoints);

		for (Tuple2<StateHandle<CompletedCheckpoint>, String> checkpoint : initialCheckpoints) {
			checkpointStateHandles.add(checkpoint);
		}
	}

	/**
	 * Synchronously writes the new checkpoints to ZooKeeper and asynchronously removes older ones.
	 *
	 * @param checkpoint Completed checkpoint to add.
	 */
	@Override
	public void addCheckpoint(CompletedCheckpoint checkpoint) throws Exception {
		checkNotNull(checkpoint, "Checkpoint");

		// First add the new one. If it fails, we don't want to loose existing data.
		String path = checkpointIdToPath(checkpoint.getCheckpointID());

		final StateHandle<CompletedCheckpoint> stateHandle = checkpointsInZooKeeper.add(path, checkpoint);

		checkpointStateHandles.addLast(new Tuple2<>(stateHandle, path));

		// Everything worked, let's remove a previous checkpoint if necessary.
		while (checkpointStateHandles.size() > maxNumberOfCheckpointsToRetain) {
			removeFromZooKeeperAndDiscardCheckpoint(checkpointStateHandles.removeFirst());
		}

		LOG.debug("Added {} to {}.", checkpoint, path);
	}

	@Override
	public CompletedCheckpoint getLatestCheckpoint() throws Exception {
		if (checkpointStateHandles.isEmpty()) {
			return null;
		}
		else {
			while(!checkpointStateHandles.isEmpty()) {
				Tuple2<StateHandle<CompletedCheckpoint>, String> checkpointStateHandle = checkpointStateHandles.peekLast();

				try {
					return retrieveCompletedCheckpoint(checkpointStateHandle);
				} catch (Exception e) {
					LOG.warn("Could not retrieve latest checkpoint. Removing it from " +
						"the completed checkpoint store.", e);

					// remove the checkpoint with broken state handle
					removeFromZooKeeperAndDiscardCheckpoint(checkpointStateHandles.pollLast());
				}
			}

			return null;
		}
	}

	@Override
	public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
		List<CompletedCheckpoint> checkpoints = new ArrayList<>(checkpointStateHandles.size());

		Iterator<Tuple2<StateHandle<CompletedCheckpoint>, String>> stateHandleIterator = checkpointStateHandles.iterator();

		while (stateHandleIterator.hasNext()) {
			Tuple2<StateHandle<CompletedCheckpoint>, String> stateHandlePath = stateHandleIterator.next();

			try {
				checkpoints.add(retrieveCompletedCheckpoint(stateHandlePath));
			} catch (Exception e) {
				LOG.warn("Could not retrieve checkpoint. Removing it from the completed " +
					"checkpoint store.", e);

				// remove the checkpoint with broken state handle
				stateHandleIterator.remove();
				removeFromZooKeeperAndDiscardCheckpoint(stateHandlePath);
			}
		}

		return checkpoints;
	}

	@Override
	public int getNumberOfRetainedCheckpoints() {
		return checkpointStateHandles.size();
	}

	@Override
	public void shutdown() throws Exception {
		LOG.info("Shutting down");

		for (Tuple2<StateHandle<CompletedCheckpoint>, String> checkpoint : checkpointStateHandles) {
			try {
				removeFromZooKeeperAndDiscardCheckpoint(checkpoint);
			}
			catch (Exception e) {
				LOG.error("Failed to discard checkpoint.", e);
			}
		}

		checkpointStateHandles.clear();

		String path = "/" + client.getNamespace();

		LOG.info("Removing {} from ZooKeeper", path);
		ZKPaths.deleteChildren(client.getZookeeperClient().getZooKeeper(), path, true);
	}

	@Override
	public void suspend() throws Exception {
		LOG.info("Suspending");

		// Clear the local handles, but don't remove any state
		checkpointStateHandles.clear();
	}

	/**
	 * Removes the state handle from ZooKeeper, discards the checkpoints, and the state handle.
	 */
	private void removeFromZooKeeperAndDiscardCheckpoint(final Tuple2<StateHandle<CompletedCheckpoint>, String> stateHandleAndPath) throws Exception {

		final BackgroundCallback callback = new BackgroundCallback() {
			@Override
			public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
				final long checkpointId = pathToCheckpointId(stateHandleAndPath.f1);

				try {
					if (event.getType() == CuratorEventType.DELETE) {
						if (event.getResultCode() == 0) {
							Exception exception = null;

							// The checkpoint
							CompletedCheckpoint checkpoint = null;

								try {
								checkpoint = stateHandleAndPath.f0.getState(userClassLoader);
								} catch (Exception e) {
								Exception newException = new Exception("Could not retrieve the completed checkpoint " +
									checkpointId + " from the state storage.", e);

								exception = ExceptionUtils.firstOrSuppressed(newException, exception);
							}

							if (checkpoint != null) {
								try {
									checkpoint.discard(userClassLoader);
								} catch (Exception e) {
									Exception newException = new Exception("Could not discard the completed checkpoint " +
										checkpoint + '.', e);

									exception = ExceptionUtils.firstOrSuppressed(newException, exception);
								}
							}

							try {
								// Discard the state handle
								stateHandleAndPath.f0.discardState();
							} catch (Exception e) {
								Exception newException = new Exception("Could not discard meta data of completed checkpoint " +
									checkpointId + '.', e);

								exception = ExceptionUtils.firstOrSuppressed(newException, exception);
							}

							if (exception != null) {
								throw exception;
							} else {
								// Discard the checkpoint
								LOG.debug("Discarded {}.", checkpoint);
							}
						}
						else {
							throw new IllegalStateException("Unexpected result code " +
									event.getResultCode() + " in '" + event + "' callback.");
						}
					}
					else {
						throw new IllegalStateException("Unexpected event type " +
								event.getType() + " in '" + event + "' callback.");
					}
				}
				catch (Exception e) {
					LOG.warn("Failed to discard checkpoint {}.", checkpointId, e);
				}
			}
		};

		// Remove state handle from ZooKeeper first. If this fails, we can still recover, but if
		// we remove a state handle and fail to remove it from ZooKeeper, we end up in an
		// inconsistent state.
		checkpointsInZooKeeper.remove(stateHandleAndPath.f1, callback);
	}

	/**
	 * Convert a checkpoint id into a ZooKeeper path.
	 *
	 * @param checkpointId to convert to the path
	 * @return Path created from the given checkpoint id
	 */
	protected static String checkpointIdToPath(long checkpointId) {
		return String.format("/%s", checkpointId);
	}

	/**
	 * Converts a path to the checkpoint id.
	 *
	 * @param path in ZooKeeper
	 * @return Checkpoint id parsed from the path
	 */
	protected static long pathToCheckpointId(String path) {
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

	private CompletedCheckpoint retrieveCompletedCheckpoint(Tuple2<StateHandle<CompletedCheckpoint>, String> stateHandlePath) throws Exception {
		long checkpointId = pathToCheckpointId(stateHandlePath.f1);

		LOG.info("Trying to retrieve checkpoint {}.", checkpointId);

		try {
			return stateHandlePath.f0.getState(userClassLoader);
		} catch (Exception e) {
			throw new Exception("Could not retrieve checkpoint " + checkpointId + ". The state handle seems to be broken.", e);
		}
	}
}
