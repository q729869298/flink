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

package org.apache.flink.client.program;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;

/**
 * The factory that create the stream environment to be used when running jobs that are
 * submitted through a pre-configured client connection.
 * This happens for example when a job is submitted from the command line.
 */
public class StreamContextEnvironmentFactory implements StreamExecutionEnvironmentFactory {

	private ContextEnvironmentFactory contextEnvironmentFactory;

	public StreamContextEnvironmentFactory(ContextEnvironmentFactory contextEnvironmentFactory) {
		this.contextEnvironmentFactory = contextEnvironmentFactory;
	}

	@Override
	public StreamExecutionEnvironment createExecutionEnvironment() {
		return new StreamContextEnvironment((ContextEnvironment)contextEnvironmentFactory.createExecutionEnvironment());
	}
}
