/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.collector.selector;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.List;
import java.util.Map;


/**
 * Special version of {@link DirectedOutput} that performs a shallow copy of the
 * {@link StreamRecord} to ensure that multi-chaining works correctly.
 */
public class CopyingDirectedOutput<OUT> extends DirectedOutput<OUT> {

	@SuppressWarnings({"unchecked", "rawtypes"})
	public CopyingDirectedOutput(
			List<OutputSelector<OUT>> outputSelectors,
			List<? extends Tuple2<? extends Output<StreamRecord<OUT>>, StreamEdge>> outputs,
			Counter numRecordsOutForTask) {
		super(outputSelectors, outputs, numRecordsOutForTask);
	}

	@Override
	protected SelectedOutputsCollector<OUT> initSelectedOutputsCollector(
			Output<StreamRecord<OUT>>[] selectAllOutputs,
			Map<String, Output<StreamRecord<OUT>>[]> outputMap) {
		return new CopyingDirectedOutputsCollector<>(selectAllOutputs, outputMap);
	}
}
