/**
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


package org.apache.flink.api.java.record.functions;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FlatJoinable;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

/**
 * The JoinFunction must implementation by functions of a {@link org.apache.flink.api.java.operators.JoinOperator}.
 * It resembles an equality join of both inputs on their key fields.
 */
public abstract class JoinFunction extends AbstractRichFunction implements FlatJoinable<Record, Record, Record> {
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * This method must be implemented to provide a user implementation of a join.
	 * It is called for each two records that share the same key and come from different inputs.
	 * 
	 * @param value1 The record that comes from the first input.
	 * @param value2 The record that comes from the second input.
	 * @return The result of the join UDF as record
	 * 
	 * @throws Exception Implementations may forward exceptions, which are caught by the runtime. When the
	 *                   runtime catches an exception, it aborts the combine task and lets the fail-over logic
	 *                   decide whether to retry the combiner execution.
	 */
	@Override
	public abstract void join(Record value1, Record value2, Collector<Record> out) throws Exception;
}
