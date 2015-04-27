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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamOperator;

/**
 * Represents the end of a DataStream.
 *
 * @param <IN>
 *            The type of the DataStream closed by the sink.
 */
public class DataStreamSink<IN> extends SingleOutputStreamOperator<IN, DataStreamSink<IN>> {

	protected DataStreamSink(StreamExecutionEnvironment environment, String operatorType,
			TypeInformation<IN> outTypeInfo, StreamOperator<?,?> operator) {
		super(environment, operatorType, outTypeInfo, operator);
	}

	protected DataStreamSink(DataStream<IN> dataStream) {
		super(dataStream);
	}

	@Override
	public DataStreamSink<IN> copy() {
		throw new RuntimeException("Data stream sinks cannot be copied");
	}

}
