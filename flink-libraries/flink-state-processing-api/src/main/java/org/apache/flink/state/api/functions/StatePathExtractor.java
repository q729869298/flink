/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.api.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.util.Collector;

/**
 * Extract from an OperatorState a set of state file paths.
 */
public class StatePathExtractor implements FlatMapFunction<OperatorState, String> {
	/**
	 * This method recursively looks for the contained {@link FileStateHandle}s in a given {@link StreamStateHandle}.
	 *
	 * @param handle the {@code StreamStateHandle} to check for a contained {@code FileStateHandle}
	 * @return the file path if the given {@code StreamStateHandle} contains a {@code FileStateHandle} object, null
	 * otherwise
	 */
	private Path getStateFilePathFromStreamStateHandle(StreamStateHandle handle) {
		if (handle instanceof FileStateHandle) {
			return ((FileStateHandle) handle).getFilePath();
		} else if (handle instanceof OperatorStateHandle) {
			return getStateFilePathFromStreamStateHandle(
				((OperatorStateHandle) handle).getDelegateStateHandle()
			);
		} else if (handle instanceof KeyedStateHandle) {
			if (handle instanceof KeyGroupsStateHandle) {
				return getStateFilePathFromStreamStateHandle(
					((KeyGroupsStateHandle) handle).getDelegateStateHandle()
				);
			}
		}
		return null;
	}

	@Override
	public void flatMap(OperatorState operatorState, Collector<String> out) throws Exception {
		Path path;
		for (OperatorSubtaskState subTaskState : operatorState.getSubtaskStates().values()) {
			// managed operator state
			for (OperatorStateHandle operatorStateHandle: subTaskState.getManagedOperatorState()) {
				path = getStateFilePathFromStreamStateHandle(operatorStateHandle);
				if (path != null) {
					out.collect(path.getPath());
				}
			}
			// managed keyed state
			for (KeyedStateHandle keyedStateHandle: subTaskState.getManagedKeyedState()) {
				if (keyedStateHandle instanceof KeyGroupsStateHandle) {
					path = getStateFilePathFromStreamStateHandle((KeyGroupsStateHandle) keyedStateHandle);
					if (path != null) {
						out.collect(path.getPath());
					}
				}
			}
			// raw operator state
			for (OperatorStateHandle operatorStateHandle: subTaskState.getRawOperatorState()) {
				path = getStateFilePathFromStreamStateHandle(operatorStateHandle);
				if (path != null) {
					out.collect(path.getPath());
				}
			}
			// raw keyed state
			for (KeyedStateHandle keyedStateHandle: subTaskState.getRawKeyedState()) {
				if (keyedStateHandle instanceof KeyGroupsStateHandle) {
					path = getStateFilePathFromStreamStateHandle((KeyGroupsStateHandle) keyedStateHandle);
					if (path != null) {
						out.collect(path.getPath());
					}
				}
			}
		}
	}

}
