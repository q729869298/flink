/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.source.coordinator.SourceCoordinatorSerdeUtils.readAndVerifyCoordinatorSerdeVersion;
import static org.apache.flink.runtime.source.coordinator.SourceCoordinatorSerdeUtils.writeCoordinatorSerdeVersion;
import static org.apache.flink.util.serde.SerdeUtils.readBytes;
import static org.apache.flink.util.serde.SerdeUtils.readInt;
import static org.apache.flink.util.serde.SerdeUtils.writeBytes;
import static org.apache.flink.util.serde.SerdeUtils.writeInt;

/**
 * The default implementation of the {@link OperatorCoordinator} for the {@link Source}.
 *
 * <p>The <code>SourceCoordinator</code> provides an event loop style thread model to interact with
 * the Flink runtime. The coordinator ensures that all the state manipulations are made by its event loop
 * thread. It also helps keep track of the necessary split assignments history per subtask to simplify the
 * {@link SplitEnumerator} implementation.
 *
 * <p>The coordinator maintains a {@link org.apache.flink.api.connector.source.SplitEnumeratorContext
 * SplitEnumeratorContxt} and shares it with the enumerator. When the coordinator receives an action
 * request from the Flink runtime, it sets up the context, and calls corresponding method of the
 * SplitEnumerator to take actions.
 */
@Internal
public class SourceCoordinator<SplitT extends SourceSplit, EnumChkT> implements OperatorCoordinator {
	private static final Logger LOG = LoggerFactory.getLogger(SourceCoordinator.class);
	/** The name of the operator this SourceCoordinator is associated with. */
	private final String operatorName;
	/** A single-thread executor to handle all the changes to the coordinator. */
	private final ExecutorService coordinatorExecutor;
	/** The Source that is associated with this SourceCoordinator. */
	private final Source<?, SplitT, EnumChkT> source;
	/** The serializer that handles the serde of the SplitEnumerator checkpoints. */
	private final SimpleVersionedSerializer<EnumChkT> enumCheckpointSerializer;
	/** The serializer for the SourceSplit of the associated Source. */
	private final SimpleVersionedSerializer<SplitT> splitSerializer;
	/** The context containing the states of the coordinator. */
	private final SourceCoordinatorContext<SplitT> context;
	/** The split enumerator created from the associated Source. */
	private SplitEnumerator<SplitT, EnumChkT> enumerator;
	/** A flag marking whether the coordinator has started. */
	private boolean started;

	public SourceCoordinator(
			String operatorName,
			ExecutorService coordinatorExecutor,
			Source<?, SplitT, EnumChkT> source,
			SourceCoordinatorContext<SplitT> context) {
		this.operatorName = operatorName;
		this.coordinatorExecutor = coordinatorExecutor;
		this.source = source;
		this.enumCheckpointSerializer = source.getEnumeratorCheckpointSerializer();
		this.splitSerializer = source.getSplitSerializer();
		this.context = context;
		this.enumerator = source.createEnumerator(context);
		this.started = false;
	}

	@Override
	public void start() throws Exception {
		LOG.info("Starting split enumerator for source {}.", operatorName);
		enumerator.start();
		started = true;
	}

	@Override
	public void close() throws Exception {
		LOG.info("Closing SourceCoordinator for source {}.", operatorName);
		try {
			if (started) {
				enumerator.close();
			}
		} finally {
			coordinatorExecutor.shutdown();
			// We do not expect this to actually block for long. At this point, there should be very few task running
			// in the executor, if any.
			coordinatorExecutor.awaitTermination(10, TimeUnit.SECONDS);
		}
		LOG.info("Source coordinator for source {} closed.", operatorName);
	}

	@Override
	public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
		ensureStarted();
		coordinatorExecutor.execute(() -> {
			try {
				LOG.debug("Handling event from subtask {} of source {}: {}", subtask, operatorName, event);
				if (event instanceof SourceEventWrapper) {
					enumerator.handleSourceEvent(subtask, ((SourceEventWrapper) event).getSourceEvent());
				} else if (event instanceof ReaderRegistrationEvent) {
					handleReaderRegistrationEvent((ReaderRegistrationEvent) event);
				}
			} catch (Throwable t) {
				LOG.error("Failing the job due to exception when handling operator event {} from subtask {} " +
								"of source {}.", event, subtask, operatorName, t);
				context.failJob(t);
			}
		});
	}

	@Override
	public void subtaskFailed(int subtaskId) {
		ensureStarted();
		coordinatorExecutor.execute(() -> {
			try {
				LOG.info("Handling subtask {} failure of source {}.", subtaskId, operatorName);
				List<SplitT> splitsToAddBack = context.getAndRemoveUncheckpointedAssignment(subtaskId);
				context.unregisterSourceReader(subtaskId);
				LOG.debug("Adding {} back to the split enumerator of source {}.", splitsToAddBack, operatorName);
				enumerator.addSplitsBack(splitsToAddBack, subtaskId);
			} catch (Throwable t) {
				LOG.error("Failing the job due to exception when handling subtask {} failure in source {}.",
						subtaskId, operatorName, t);
				context.failJob(t);
			}
		});
	}

	@Override
	public CompletableFuture<byte[]> checkpointCoordinator(long checkpointId) throws Exception {
		ensureStarted();
		return CompletableFuture.supplyAsync(() -> {
			try {
				LOG.debug("Taking a state snapshot on operator {} for checkpoint {}", operatorName, checkpointId);
				return toBytes(checkpointId);
			} catch (Exception e) {
				throw new CompletionException(
						String.format("Failed to checkpoint coordinator for source %s due to ", operatorName), e);
			}
		}, coordinatorExecutor);
	}

	@Override
	public void checkpointComplete(long checkpointId) {
		ensureStarted();
		coordinatorExecutor.execute(() -> {
			try {
				LOG.info("Marking checkpoint {} as completed for source {}.", checkpointId, operatorName);
				context.onCheckpointComplete(checkpointId);
			} catch (Throwable t) {
				LOG.error("Failing the job due to exception when completing the checkpoint {} for source {}.",
						checkpointId, operatorName, t);
				context.failJob(t);
			}
		});
	}

	@Override
	public void resetToCheckpoint(byte[] checkpointData) throws Exception {
		if (started) {
			throw new IllegalStateException(String.format(
					"The coordinator for source %s has started. The source coordinator state can " +
					"only be reset to a checkpoint before it starts.", operatorName));
		}
		LOG.info("Resetting coordinator of source {} from checkpoint.", operatorName);
		if (started) {
			enumerator.close();
		}
		LOG.info("Resetting SourceCoordinator from checkpoint.");
		fromBytes(checkpointData);
	}

	// ---------------------------------------------------
	@VisibleForTesting
	SplitEnumerator<SplitT, EnumChkT> getEnumerator() {
		return enumerator;
	}

	@VisibleForTesting
	SourceCoordinatorContext<SplitT> getContext() {
		return context;
	}

	// --------------------- Serde -----------------------

	/**
	 * Serialize the coordinator state. The current implementation may not be super efficient,
	 * but it should not matter that much because most of the state should be rather small.
	 * Large states themselves may already be a problem regardless of how the serialization
	 * is implemented.
	 *
	 * @return A byte array containing the serialized state of the source coordinator.
	 * @throws Exception When something goes wrong in serialization.
	 */
	private byte[] toBytes(long checkpointId) throws Exception {
		EnumChkT enumCkpt = enumerator.snapshotState();

		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			writeCoordinatorSerdeVersion(out);
			writeInt(enumCheckpointSerializer.getVersion(), out);
			writeBytes(enumCheckpointSerializer.serialize(enumCkpt), out);
			context.snapshotState(checkpointId, splitSerializer, out);
			out.flush();
			return out.toByteArray();
		}
	}

	/**
	 * Restore the state of this source coordinator from the state bytes.
	 *
	 * @param bytes The checkpoint bytes that was returned from {@link #toBytes(long)}
	 * @throws Exception When the deserialization failed.
	 */
	private void fromBytes(byte[] bytes) throws Exception {
		try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
			readAndVerifyCoordinatorSerdeVersion(in);
			int enumSerializerVersion = readInt(in);
			byte[] serializedEnumChkpt = readBytes(in);
			EnumChkT enumChkpt = enumCheckpointSerializer.deserialize(enumSerializerVersion, serializedEnumChkpt);
			context.restoreState(splitSerializer, in);
			enumerator = source.restoreEnumerator(context, enumChkpt);
		}
	}

	// --------------------- private methods -------------

	private void handleReaderRegistrationEvent(ReaderRegistrationEvent event) {
		context.registerSourceReader(new ReaderInfo(event.subtaskId(), event.location()));
		enumerator.addReader(event.subtaskId());
	}

	private void ensureStarted() {
		if (!started) {
			throw new IllegalStateException("The coordinator has not started yet.");
		}
	}
}
