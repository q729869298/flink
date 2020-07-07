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

package org.apache.flink.table.runtime.functions.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.core.JoinRelType;

/**
 * The {@link RichFlatMapFunction} used to invoke Python {@link TableFunction} functions for the
 * old planner.
 */
@Internal
public final class PythonTableFunctionFlatMap extends AbstractPythonStatelessFunctionFlatMap {

	private static final long serialVersionUID = 1L;

	private static final String TABLE_FUNCTION_SCHEMA_CODER_URN = "flink:coder:schema:table_function:v1";

	private static final String TABLE_FUNCTION_URN = "flink:transform:table_function:v1";

	/**
	 * The Python {@link TableFunction} to be executed.
	 */
	private final PythonFunctionInfo tableFunction;

	/**
	 * The correlate join type.
	 */
	private final JoinRelType joinType;

	public PythonTableFunctionFlatMap(
		Configuration config,
		PythonFunctionInfo tableFunction,
		RowType inputType,
		RowType outputType,
		int[] udtfInputOffsets,
		JoinRelType joinType) {
		super(config, inputType, outputType, udtfInputOffsets);
		this.tableFunction = Preconditions.checkNotNull(tableFunction);
		Preconditions.checkArgument(
			joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT,
			"The join type should be inner join or left join");
		this.joinType = joinType;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		RowTypeInfo forwardedInputTypeInfo = (RowTypeInfo) TypeConversions.fromDataTypeToLegacyInfo(
			TypeConversions.fromLogicalToDataType(inputType));
		forwardedInputSerializer = forwardedInputTypeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
	}

	@Override
	public PythonEnv getPythonEnv() {
		return tableFunction.getPythonFunction().getPythonEnv();
	}

	@Override
	public void bufferInput(Row input) {
		// If the input node is a DataSetCalc node, the RichFlatMapFunction generated by codegen
		// will reuse the output Row, so here we always copy the input Row to solve this problem.
		input = forwardedInputSerializer.copy(input);
		forwardedInputQueue.add(input);
	}

	@Override
	@SuppressWarnings("ConstantConditions")
	public void emitResult() throws Exception {
		Row input = forwardedInputQueue.poll();
		byte[] rawUdtfResult;
		int length;
		boolean isFinishResult;
		boolean hasJoined = false;
		Row udtfResult;
		do {
			rawUdtfResult = resultTuple.f0;
			length = resultTuple.f1;
			isFinishResult = isFinishResult(rawUdtfResult, length);
			if (!isFinishResult) {
				bais.setBuffer(rawUdtfResult, 0, length);
				udtfResult = userDefinedFunctionTypeSerializer.deserialize(baisWrapper);
				this.resultCollector.collect(Row.join(input, udtfResult));
				resultTuple = pythonFunctionRunner.receive();
				hasJoined = true;
			} else if (joinType == JoinRelType.LEFT && !hasJoined) {
				udtfResult = new Row(userDefinedFunctionOutputType.getFieldCount());
				for (int i = 0; i < udtfResult.getArity(); i++) {
					udtfResult.setField(0, null);
				}
				this.resultCollector.collect(Row.join(input, udtfResult));
			}
		} while (!isFinishResult);
	}

	@Override
	public int getForwardedFieldsCount() {
		return inputType.getFieldCount();
	}

	@Override
	public FlinkFnApi.UserDefinedFunctions getUserDefinedFunctionsProto() {
		FlinkFnApi.UserDefinedFunctions.Builder builder = FlinkFnApi.UserDefinedFunctions.newBuilder();
		builder.addUdfs(getUserDefinedFunctionProto(tableFunction));
		builder.setMetricEnabled(getPythonConfig().isMetricEnabled());
		return builder.build();
	}

	@Override
	public String getInputOutputCoderUrn() {
		return TABLE_FUNCTION_SCHEMA_CODER_URN;
	}

	@Override
	public String getFunctionUrn() {
		return TABLE_FUNCTION_URN;
	}

	private boolean isFinishResult(byte[] rawUdtfResult, int length) {
		return length == 1 && rawUdtfResult[0] == 0x00;
	}
}
