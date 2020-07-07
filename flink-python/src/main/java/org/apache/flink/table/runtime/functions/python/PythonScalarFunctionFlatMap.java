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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * The {@link RichFlatMapFunction} used to invoke Python {@link ScalarFunction} functions for the
 * old planner.
 */
@Internal
public class PythonScalarFunctionFlatMap extends AbstractPythonScalarFunctionFlatMap {

	private static final long serialVersionUID = 1L;

	private static final String SCALAR_FUNCTION_SCHEMA_CODER_URN = "flink:coder:schema:scalar_function:v1";

	public PythonScalarFunctionFlatMap(
		Configuration config,
		PythonFunctionInfo[] scalarFunctions,
		RowType inputType,
		RowType outputType,
		int[] udfInputOffsets,
		int[] forwardedFields) {
		super(config, scalarFunctions, inputType, outputType, udfInputOffsets, forwardedFields);
	}

	@Override
	public String getInputOutputCoderUrn() {
		return SCALAR_FUNCTION_SCHEMA_CODER_URN;
	}

	@Override
	@SuppressWarnings("ConstantConditions")
	public void emitResult() throws IOException {
		byte[] rawUdfResult = resultTuple.f0;
		int length = resultTuple.f1;
		Row input = forwardedInputQueue.poll();
		bais.setBuffer(rawUdfResult, 0, length);
		Row udfResult = userDefinedFunctionTypeSerializer.deserialize(baisWrapper);
		this.resultCollector.collect(Row.join(input, udfResult));
	}
}
