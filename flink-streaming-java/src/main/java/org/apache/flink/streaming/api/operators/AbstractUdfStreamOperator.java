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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.InstantiationUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RunnableFuture;

import static java.util.Objects.requireNonNull;

/**
 * This is used as the base class for operators that have a user-defined
 * function. This class handles the opening and closing of the user-defined functions,
 * as part of the operator life cycle.
 * 
 * @param <OUT>
 *            The output type of the operator
 * @param <F>
 *            The type of the user function
 */
@PublicEvolving
public abstract class AbstractUdfStreamOperator<OUT, F extends Function>
		extends AbstractStreamOperator<OUT>
		implements OutputTypeConfigurable<OUT>,
		StreamCheckpointedOperator {

	private static final long serialVersionUID = 1L;
	
	
	/** the user function */
	protected final F userFunction;
	
	/** Flag to prevent duplicate function.close() calls in close() and dispose() */
	private transient boolean functionsClosed = false;
	
	public AbstractUdfStreamOperator(F userFunction) {
		this.userFunction = requireNonNull(userFunction);
	}

	/**
	 * Gets the user function executed in this operator.
	 * @return The user function of this operator.
	 */
	public F getUserFunction() {
		return userFunction;
	}
	
	// ------------------------------------------------------------------------
	//  operator life cycle
	// ------------------------------------------------------------------------


	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output, boolean isSink) {
		super.setup(containingTask, config, output, isSink);
		
		FunctionUtils.setFunctionRuntimeContext(userFunction, getRuntimeContext());
	}

	@Override
	public void open() throws Exception {
		super.open();
		
		FunctionUtils.openFunction(userFunction, new Configuration());

		if (userFunction instanceof CheckpointedFunction) {
			((CheckpointedFunction) userFunction).initializeState(getOperatorStateBackend());
		} else if (userFunction instanceof ListCheckpointed) {
			@SuppressWarnings("unchecked")
			ListCheckpointed<Serializable> listCheckpointedFun = (ListCheckpointed<Serializable>) userFunction;

			ListState<Serializable> listState = getOperatorStateBackend().
					getSerializableListState(OperatorStateStore.DEFAULT_OPERATOR_STATE_NAME);

			List<Serializable> list = new ArrayList<>();

			for (Serializable serializable : listState.get()) {
				list.add(serializable);
			}

			try {
				listCheckpointedFun.restoreState(list);
			} catch (Exception e) {
				throw new Exception("Failed to restore state to function: " + e.getMessage(), e);
			}
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		functionsClosed = true;
		FunctionUtils.closeFunction(userFunction);
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		if (!functionsClosed) {
			functionsClosed = true;
			FunctionUtils.closeFunction(userFunction);
		}
	}

	// ------------------------------------------------------------------------
	//  checkpointing and recovery
	// ------------------------------------------------------------------------
	
	@Override
	public void snapshotState(FSDataOutputStream out, long checkpointId, long timestamp) throws Exception {

		if (userFunction instanceof Checkpointed) {
			@SuppressWarnings("unchecked")
			Checkpointed<Serializable> chkFunction = (Checkpointed<Serializable>) userFunction;
			
			Serializable udfState;
			try {
				udfState = chkFunction.snapshotState(checkpointId, timestamp);
				if (udfState != null) {
					out.write(1);
					InstantiationUtil.serializeObject(out, udfState);
				} else {
					out.write(0);
				}
			} catch (Exception e) {
				throw new Exception("Failed to draw state snapshot from function: " + e.getMessage(), e);
			}
		}
	}

	@Override
	public void restoreState(FSDataInputStream in) throws Exception {

		if (userFunction instanceof Checkpointed) {
			@SuppressWarnings("unchecked")
			Checkpointed<Serializable> chkFunction = (Checkpointed<Serializable>) userFunction;

			int hasUdfState = in.read();

			if (hasUdfState == 1) {
				Serializable functionState = InstantiationUtil.deserializeObject(in, getUserCodeClassloader());
				if (functionState != null) {
					try {
						chkFunction.restoreState(functionState);
					} catch (Exception e) {
						throw new Exception("Failed to restore state to function: " + e.getMessage(), e);
					}
				}
			}
		}
	}

	@Override
	public RunnableFuture<OperatorStateHandle> snapshotState(
			long checkpointId, long timestamp, CheckpointStreamFactory streamFactory) throws Exception {

		if (userFunction instanceof CheckpointedFunction) {
			((CheckpointedFunction) userFunction).prepareSnapshot(checkpointId, timestamp);
		}

		if (userFunction instanceof ListCheckpointed) {
			@SuppressWarnings("unchecked")
			List<Serializable> partitionableState =
					((ListCheckpointed<Serializable>) userFunction).snapshotState(checkpointId, timestamp);

			ListState<Serializable> listState = getOperatorStateBackend().
					getSerializableListState(OperatorStateStore.DEFAULT_OPERATOR_STATE_NAME);

			listState.clear();

			for (Serializable statePartition : partitionableState) {
				listState.add(statePartition);
			}
		}

		return super.snapshotState(checkpointId, timestamp, streamFactory);
	}

	@Override
	public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
		super.notifyOfCompletedCheckpoint(checkpointId);

		if (userFunction instanceof CheckpointListener) {
			((CheckpointListener) userFunction).notifyCheckpointComplete(checkpointId);
		}
	}

	// ------------------------------------------------------------------------
	//  Output type configuration
	// ------------------------------------------------------------------------

	@Override
	public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
		if (userFunction instanceof OutputTypeConfigurable) {
			@SuppressWarnings("unchecked")
			OutputTypeConfigurable<OUT> outputTypeConfigurable = (OutputTypeConfigurable<OUT>) userFunction;
			outputTypeConfigurable.setOutputType(outTypeInfo, executionConfig);
		}
	}


	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * 
	 * Since the streaming API does not implement any parametrization of functions via a
	 * configuration, the config returned here is actually empty.
	 * 
	 * @return The user function parameters (currently empty)
	 */
	public Configuration getUserFunctionParameters() {
		return new Configuration();
	}
}
