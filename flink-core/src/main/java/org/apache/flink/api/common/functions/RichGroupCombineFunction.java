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

package org.apache.flink.api.common.functions;


import org.apache.flink.util.Collector;

/**
 * Rich variant of the {@link GroupCombineFunction}. As a {@link RichFunction}, it gives access to the
 * {@link org.apache.flink.api.common.functions.RuntimeContext} and provides setup and teardown methods:
 * {@link RichFunction#open(org.apache.flink.configuration.Configuration)} and
 * {@link RichFunction#close()}.
 *
 * @param <IN> The data type of the elements to be combined.
 * @param <OUT> The resulting data type of the elements to be combined.
 */
public abstract class RichGroupCombineFunction<IN, OUT> extends AbstractRichFunction implements GroupCombineFunction<IN, OUT> {

	private static final long serialVersionUID = 1L;

	@Override
	public abstract void combine(Iterable<IN> values, Collector<OUT> out) throws Exception;
}
