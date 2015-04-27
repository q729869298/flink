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


package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.GenericCollectorMap;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Map task which is executed by a Task Manager. The task has a single
 * input and one or multiple outputs. It is provided with a MapFunction
 * implementation.
 * <p>
 * The MapTask creates an iterator over all key-value pairs of its input and hands that to the <code>map()</code> method
 * of the MapFunction.
 * 
 * @see GenericCollectorMap
 * 
 * @param <IT> The mapper's input data type.
 * @param <OT> The mapper's output data type.
 */
@SuppressWarnings("deprecation")
public class CollectorMapDriver<IT, OT> implements PactDriver<GenericCollectorMap<IT, OT>, OT> {

	private static final Logger LOG = LoggerFactory.getLogger(CollectorMapDriver.class);


	private PactTaskContext<GenericCollectorMap<IT, OT>, OT> taskContext;
	
	private volatile boolean running;

	private boolean objectReuseEnabled = false;

	@Override
	public void setup(PactTaskContext<GenericCollectorMap<IT, OT>, OT> context) {
		this.taskContext = context;
		this.running = true;
	}

	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<GenericCollectorMap<IT, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GenericCollectorMap<IT, OT>> clazz = (Class<GenericCollectorMap<IT, OT>>) (Class<?>) GenericCollectorMap.class;
		return clazz;
	}

	@Override
	public int getNumberOfDriverComparators() {
		return 0;
	}

	@Override
	public void prepare() {
		ExecutionConfig executionConfig = taskContext.getExecutionConfig();
		this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();

		if (LOG.isDebugEnabled()) {
			LOG.debug("CollectorMapDriver object reuse: " + (this.objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");
		}
	}

	@Override
	public void run() throws Exception {
		// cache references on the stack
		final MutableObjectIterator<IT> input = this.taskContext.getInput(0);
		final GenericCollectorMap<IT, OT> stub = this.taskContext.getStub();
		final Collector<OT> output = this.taskContext.getOutputCollector();

		if (objectReuseEnabled) {
			IT record = this.taskContext.<IT>getInputSerializer(0).getSerializer().createInstance();


			while (this.running && ((record = input.next(record)) != null)) {
				stub.map(record, output);
			}
		} else {
			IT record;
			while (this.running && ((record = input.next()) != null)) {
				stub.map(record, output);
			}
		}
	}

	@Override
	public void cleanup() {
		// mappers need no cleanup, since no strategies are used.
	}

	@Override
	public void cancel() {
		this.running = false;
	}
}
