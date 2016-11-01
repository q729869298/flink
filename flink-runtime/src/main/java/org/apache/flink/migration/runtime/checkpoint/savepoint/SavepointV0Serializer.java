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

package org.apache.flink.migration.runtime.checkpoint.savepoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.migration.runtime.checkpoint.KeyGroupState;
import org.apache.flink.migration.runtime.checkpoint.SubtaskState;
import org.apache.flink.migration.runtime.checkpoint.TaskState;
import org.apache.flink.migration.runtime.state.KvStateSnapshot;
import org.apache.flink.migration.runtime.state.StateHandle;
import org.apache.flink.migration.runtime.state.filesystem.AbstractFileStateHandle;
import org.apache.flink.migration.state.MigrationKeyGroupStateHandle;
import org.apache.flink.migration.state.MigrationStreamStateHandle;
import org.apache.flink.migration.streaming.runtime.tasks.StreamTaskState;
import org.apache.flink.migration.streaming.runtime.tasks.StreamTaskStateList;
import org.apache.flink.migration.util.SerializedValue;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointSerializer;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV1;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.util.InstantiationUtil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 * <p>
 * <p>In contrast to previous savepoint versions, this serializer makes sure
 * that no default Java serialization is used for serialization. Therefore, we
 * don't rely on any involved Java classes to stay the same.
 */
public class SavepointV0Serializer implements SavepointSerializer<SavepointV1> {

	public static final SavepointV0Serializer INSTANCE = new SavepointV0Serializer();

	private static final int MAX_SIZE = 16 * 1024 * 1024;

	private ClassLoader userClassLoader;
	private JobID jobID;
	private long checkpointID;

	private SavepointV0Serializer() {
	}

	@Override
	public void serialize(SavepointV1 savepoint, DataOutputStream dos) throws IOException {
		throw new UnsupportedOperationException("This serializer is read-only and only exists for backwards compatibility");
	}

	@Override
	public SavepointV1 deserialize(DataInputStream dis, ClassLoader userClassLoader) throws IOException {

		this.checkpointID = dis.readLong();
		this.userClassLoader = userClassLoader;

		// Task states
		int numTaskStates = dis.readInt();
		List<TaskState> taskStates = new ArrayList<>(numTaskStates);

		for (int i = 0; i < numTaskStates; i++) {
			JobVertexID jobVertexId = new JobVertexID(dis.readLong(), dis.readLong());
			int parallelism = dis.readInt();

			// Add task state
			TaskState taskState = new TaskState(jobVertexId, parallelism);
			taskStates.add(taskState);

			// Sub task states
			int numSubTaskStates = dis.readInt();
			for (int j = 0; j < numSubTaskStates; j++) {
				int subtaskIndex = dis.readInt();

				int length = dis.readInt();

				SerializedValue<StateHandle<?>> serializedValue;
				if (length == -1) {
					serializedValue = new SerializedValue<>(null);
				} else {
					byte[] serializedData = new byte[length];
					dis.readFully(serializedData, 0, length);
					serializedValue = SerializedValue.fromBytes(serializedData);
				}

				long stateSize = dis.readLong();
				long duration = dis.readLong();

				SubtaskState subtaskState = new SubtaskState(
						serializedValue,
						stateSize,
						duration);

				taskState.putState(subtaskIndex, subtaskState);
			}

			// Key group states
			int numKvStates = dis.readInt();
			for (int j = 0; j < numKvStates; j++) {
				int keyGroupIndex = dis.readInt();

				int length = dis.readInt();

				SerializedValue<StateHandle<?>> serializedValue;
				if (length == -1) {
					serializedValue = new SerializedValue<>(null);
				} else {
					byte[] serializedData = new byte[length];
					dis.readFully(serializedData, 0, length);
					serializedValue = SerializedValue.fromBytes(serializedData);
				}

				long stateSize = dis.readLong();
				long duration = dis.readLong();

				KeyGroupState keyGroupState = new KeyGroupState(
						serializedValue,
						stateSize,
						duration);

				taskState.putKvState(keyGroupIndex, keyGroupState);
			}
		}

		try {
			return convertSavepoint(taskStates);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public SavepointV1 convertSavepoint(List<TaskState> taskStates) throws Exception {

		jobID = new JobID();

		List<org.apache.flink.runtime.checkpoint.TaskState> newTaskStates = new ArrayList<>(taskStates.size());

		for (TaskState taskState : taskStates) {
			newTaskStates.add(convertTaskState(taskState));
		}

		SavepointV1 savepointV1 = new SavepointV1(checkpointID, newTaskStates);
		return savepointV1;
	}

	public org.apache.flink.runtime.checkpoint.TaskState convertTaskState(TaskState taskState) throws Exception {

		JobVertexID jobVertexID = taskState.getJobVertexID();
		int parallelism = taskState.getParallelism();
		int chainLength = determineOperatorChainLength(taskState);

		org.apache.flink.runtime.checkpoint.TaskState newTaskState =
				new org.apache.flink.runtime.checkpoint.TaskState(
						jobVertexID,
						parallelism,
						parallelism,
						chainLength);

		if (chainLength > 0) {

			int parallelInstanceIdx = 0;
			Collection<SubtaskState> subtaskStates = taskState.getStates();

			for (SubtaskState subtaskState : subtaskStates) {
				newTaskState.putState(parallelInstanceIdx, convertSubtaskState(subtaskState, parallelInstanceIdx));
				++parallelInstanceIdx;
			}
		}

		return newTaskState;
	}

	private org.apache.flink.runtime.checkpoint.SubtaskState convertSubtaskState(
			SubtaskState subtaskState, int parallelInstanceIdx) throws Exception {

		SerializedValue<StateHandle<?>> serializedValue = subtaskState.getState();

		StreamTaskStateList stateList = (StreamTaskStateList) serializedValue.deserializeValue(userClassLoader);
		StreamTaskState[] streamTaskStates = stateList.getState(userClassLoader);

		List<StreamStateHandle> newChainStateList = Arrays.asList(new StreamStateHandle[streamTaskStates.length]);
		KeyGroupsStateHandle newKeyedState = null;

		for (int chainIdx = 0; chainIdx < streamTaskStates.length; ++chainIdx) {

			StreamTaskState streamTaskState = streamTaskStates[chainIdx];
			if (streamTaskState == null) {
				continue;
			}

			newChainStateList.set(chainIdx, convertOperatorAndFunctionState(streamTaskState));
			newKeyedState = convertKeyedBackendState(streamTaskState, parallelInstanceIdx);
		}

		ChainedStateHandle<StreamStateHandle> newChainedState = new ChainedStateHandle<>(newChainStateList);
		ChainedStateHandle<OperatorStateHandle> nopChain =
				new ChainedStateHandle<>(Arrays.asList(new OperatorStateHandle[newChainedState.getLength()]));

		return new org.apache.flink.runtime.checkpoint.SubtaskState(
				newChainedState,
				nopChain,
				nopChain,
				newKeyedState,
				null);
	}

	private StreamStateHandle convertOperatorAndFunctionState(StreamTaskState streamTaskState) throws Exception {

		StateHandle<Serializable> functionState = streamTaskState.getFunctionState();
		StateHandle<?> operatorState = streamTaskState.getOperatorState();

		Path path = null;
		if (functionState instanceof AbstractFileStateHandle) {
			path = ((AbstractFileStateHandle) functionState).getFilePath();
		} else if (operatorState instanceof AbstractFileStateHandle) {
			path = ((AbstractFileStateHandle) operatorState).getFilePath();
		}

		CheckpointStreamFactory checkpointStreamFactory;

		if (path != null) {
			checkpointStreamFactory = new FsCheckpointStreamFactory(path.getParent(), jobID, 1024 * 1024);
		} else {
			checkpointStreamFactory = new MemCheckpointStreamFactory(MAX_SIZE);
		}

		CheckpointStreamFactory.CheckpointStateOutputStream opStateOut =
				checkpointStreamFactory.createCheckpointStateOutputStream(checkpointID, 0L);

		if (null != functionState) {
			opStateOut.write(1);
			InstantiationUtil.serializeObject(opStateOut, functionState.getState(userClassLoader));
		} else {
			opStateOut.write(0);
		}

		if (null != operatorState) {
			opStateOut.write(1);
			InstantiationUtil.serializeObject(opStateOut, operatorState.getState(userClassLoader));
		} else {
			opStateOut.write(0);
		}

		return opStateOut != null ? new MigrationStreamStateHandle(opStateOut.closeAndGetHandle()) : null;
	}

	private KeyGroupsStateHandle convertKeyedBackendState(
			StreamTaskState streamTaskState, int parallelInstanceIdx) throws Exception {

		HashMap<String, KvStateSnapshot<?, ?, ?, ?, ?>> oldKeyedState = streamTaskState.getKvStates();

		if (null != oldKeyedState) {

			CheckpointStreamFactory checkpointStreamFactory = new MemCheckpointStreamFactory(MAX_SIZE);

			CheckpointStreamFactory.CheckpointStateOutputStream keyedStateOut =
					checkpointStreamFactory.createCheckpointStateOutputStream(checkpointID, 0L);

			final long offset = keyedStateOut.getPos();

			InstantiationUtil.serializeObject(keyedStateOut, oldKeyedState);
			StreamStateHandle streamStateHandle = keyedStateOut.closeAndGetHandle();

			if (null != streamStateHandle) {
				KeyGroupRangeOffsets keyGroupRangeOffsets =
						new KeyGroupRangeOffsets(parallelInstanceIdx, parallelInstanceIdx, new long[]{offset});

				return new MigrationKeyGroupStateHandle(keyGroupRangeOffsets, streamStateHandle);
			}
		}
		return null;
	}

	private int determineOperatorChainLength(TaskState taskState) throws IOException, ClassNotFoundException {
		Collection<SubtaskState> subtaskStates = taskState.getStates();

		if (subtaskStates == null || subtaskStates.isEmpty()) {
			return 0;
		}

		SubtaskState firstSubtaskState = subtaskStates.iterator().next();
		Object toCastTaskStateList = firstSubtaskState.getState().deserializeValue(userClassLoader);

		if (toCastTaskStateList instanceof StreamTaskStateList) {
			StreamTaskStateList taskStateList = (StreamTaskStateList) toCastTaskStateList;
			StreamTaskState[] streamTaskStates = taskStateList.getState(userClassLoader);

			return streamTaskStates.length;
		}

		return 0;
	}
}
