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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.collector.selector.CopyingDirectedOutput;
import org.apache.flink.streaming.api.collector.selector.DirectedOutput;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.XORShiftRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * The {@code OperatorChain} contains all operators that are executed as one chain within a single
 * {@link StreamTask}.
 * 
 * @param <OUT> The type of elements accepted by the chain, i.e., the input type of the chain's
 *              head operator.
 */
@Internal
public class OperatorChain<OUT, OP extends StreamOperator<OUT>> {
	
	private static final Logger LOG = LoggerFactory.getLogger(OperatorChain.class);
	
	private final StreamOperator<?>[] allOperators;

	private final RecordWriterOutput<?>[] streamOutputs;
	
	private final Output<StreamRecord<OUT>> chainEntryPoint;

	private final OP headOperator;

	public OperatorChain(StreamTask<OUT, OP> containingTask) {
		
		final ClassLoader userCodeClassloader = containingTask.getUserCodeClassLoader();
		final StreamConfig configuration = containingTask.getConfiguration();

		headOperator = configuration.getStreamOperator(userCodeClassloader);

		// we read the chained configs, and the order of record writer registrations by output name
		Map<Integer, StreamConfig> chainedConfigs = configuration.getTransitiveChainedTaskConfigs(userCodeClassloader);
		chainedConfigs.put(configuration.getVertexID(), configuration);

		// create the final output stream writers
		// we iterate through all the out edges from this job vertex and create a stream output
		List<StreamEdge> outEdgesInOrder = configuration.getOutEdgesInOrder(userCodeClassloader);
		Map<StreamEdge, RecordWriterOutput<?>> streamOutputMap = new HashMap<>(outEdgesInOrder.size());
		this.streamOutputs = new RecordWriterOutput<?>[outEdgesInOrder.size()];
		
		// from here on, we need to make sure that the output writers are shut down again on failure
		boolean success = false;
		try {
			for (int i = 0; i < outEdgesInOrder.size(); i++) {
				StreamEdge outEdge = outEdgesInOrder.get(i);



				RecordWriterOutput<?> streamOutput = createStreamOutput(
						outEdge, chainedConfigs.get(outEdge.getSourceId()), i,
						containingTask.getEnvironment(), containingTask.getName());
	
				this.streamOutputs[i] = streamOutput;
				streamOutputMap.put(outEdge, streamOutput);
			}
	
			// we create the chain of operators and grab the collector that leads into the chain
			List<StreamOperator<?>> allOps = new ArrayList<>(chainedConfigs.size());
			this.chainEntryPoint = createOutputCollector(containingTask, configuration,
					chainedConfigs, userCodeClassloader, streamOutputMap, allOps);

			if (headOperator != null) {
				headOperator.setup(containingTask, configuration, getChainEntryPoint());
			}

			// add head operator to end of chain
			allOps.add(headOperator);

			this.allOperators = allOps.toArray(new StreamOperator<?>[allOps.size()]);
			
			success = true;
		}
		finally {
			// make sure we clean up after ourselves in case of a failure after acquiring
			// the first resources
			if (!success) {
				for (RecordWriterOutput<?> output : this.streamOutputs) {
					if (output != null) {
						output.close();
						output.clearBuffers();
					}
				}
			}
		}
		
	}


	public void broadcastCheckpointBarrier(long id, long timestamp) throws IOException {
		try {
			CheckpointBarrier barrier = new CheckpointBarrier(id, timestamp);
			for (RecordWriterOutput<?> streamOutput : streamOutputs) {
				streamOutput.broadcastEvent(barrier);
			}
		}
		catch (InterruptedException e) {
			throw new IOException("Interrupted while broadcasting checkpoint barrier");
		}
	}

	public void broadcastCheckpointCancelMarker(long id) throws IOException {
		try {
			CancelCheckpointMarker barrier = new CancelCheckpointMarker(id);
			for (RecordWriterOutput<?> streamOutput : streamOutputs) {
				streamOutput.broadcastEvent(barrier);
			}
		}
		catch (InterruptedException e) {
			throw new IOException("Interrupted while broadcasting checkpoint cancellation");
		}
	}

	public RecordWriterOutput<?>[] getStreamOutputs() {
		return streamOutputs;
	}
	
	public StreamOperator<?>[] getAllOperators() {
		return allOperators;
	}

	public Output<StreamRecord<OUT>> getChainEntryPoint() {
		return chainEntryPoint;
	}

	/**
	 *
	 * This method should be called before finishing the record emission, to make sure any data
	 * that is still buffered will be sent. It also ensures that all data sending related
	 * exceptions are recognized.
	 *
	 * @throws IOException Thrown, if the buffered data cannot be pushed into the output streams.
	 */
	public void flushOutputs() throws IOException {
		for (RecordWriterOutput<?> streamOutput : getStreamOutputs()) {
			streamOutput.flush();
		}
	}

	/**
	 * This method releases all resources of the record writer output. It stops the output
	 * flushing thread (if there is one) and releases all buffers currently held by the output
	 * serializers.
	 *
	 * <p>This method should never fail.
	 */
	public void releaseOutputs() {
		try {
			for (RecordWriterOutput<?> streamOutput : streamOutputs) {
				streamOutput.close();
			}
		}
		finally {
			// make sure that we release the buffers in any case
			for (RecordWriterOutput<?> output : streamOutputs) {
				output.clearBuffers();
			}
		}
	}

	public OP getHeadOperator() {
		return headOperator;
	}

	public int getChainLength() {
		return allOperators == null ? 0 : allOperators.length;
	}

	// ------------------------------------------------------------------------
	//  initialization utilities
	// ------------------------------------------------------------------------
	
	private static <T> Output<StreamRecord<T>> createOutputCollector(
			StreamTask<?, ?> containingTask,
			StreamConfig operatorConfig,
			Map<Integer, StreamConfig> chainedConfigs,
			ClassLoader userCodeClassloader,
			Map<StreamEdge, RecordWriterOutput<?>> streamOutputs,
			List<StreamOperator<?>> allOperators)
	{
		List<Tuple2<Output<StreamRecord<T>>, StreamEdge>> allOutputs = new ArrayList<>(4);
		
		// create collectors for the network outputs
		for (StreamEdge outputEdge : operatorConfig.getNonChainedOutputs(userCodeClassloader)) {
			@SuppressWarnings("unchecked")
			RecordWriterOutput<T> output = (RecordWriterOutput<T>) streamOutputs.get(outputEdge);
			
			allOutputs.add(new Tuple2<Output<StreamRecord<T>>, StreamEdge>(output, outputEdge));
		}

		// Create collectors for the chained outputs
		for (StreamEdge outputEdge : operatorConfig.getChainedOutputs(userCodeClassloader)) {
			int outputId = outputEdge.getTargetId();
			StreamConfig chainedOpConfig = chainedConfigs.get(outputId);

			Output<StreamRecord<T>> output = createChainedOperator(
					containingTask, chainedOpConfig, chainedConfigs, userCodeClassloader, streamOutputs, allOperators);
			allOutputs.add(new Tuple2<>(output, outputEdge));
		}
		
		// if there are multiple outputs, or the outputs are directed, we need to
		// wrap them as one output
		
		List<OutputSelector<T>> selectors = operatorConfig.getOutputSelectors(userCodeClassloader);
		
		if (selectors == null || selectors.isEmpty()) {
			// simple path, no selector necessary
			if (allOutputs.size() == 1) {
				return allOutputs.get(0).f0;
			}
			else {
				// send to N outputs. Note that this includes teh special case
				// of sending to zero outputs
				@SuppressWarnings({"unchecked", "rawtypes"})
				Output<StreamRecord<T>>[] asArray = new Output[allOutputs.size()];
				for (int i = 0; i < allOutputs.size(); i++) {
					asArray[i] = allOutputs.get(i).f0;
				}

				// This is the inverse of creating the normal ChainingOutput.
				// If the chaining output does not copy we need to copy in the broadcast output,
				// otherwise multi-chaining would not work correctly.
				if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
					return new CopyingBroadcastingOutputCollector<>(asArray);
				} else  {
					return new BroadcastingOutputCollector<>(asArray);
				}
			}
		}
		else {
			// selector present, more complex routing necessary

			// This is the inverse of creating the normal ChainingOutput.
			// If the chaining output does not copy we need to copy in the broadcast output,
			// otherwise multi-chaining would not work correctly.
			if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
				return new CopyingDirectedOutput<>(selectors, allOutputs);
			} else {
				return new DirectedOutput<>(selectors, allOutputs);
			}
			
		}
	}
	
	private static <IN, OUT> Output<StreamRecord<IN>> createChainedOperator(
			StreamTask<?, ?> containingTask,
			StreamConfig operatorConfig,
			Map<Integer, StreamConfig> chainedConfigs,
			ClassLoader userCodeClassloader,
			Map<StreamEdge, RecordWriterOutput<?>> streamOutputs,
			List<StreamOperator<?>> allOperators)
	{
		// create the output that the operator writes to first. this may recursively create more operators
		Output<StreamRecord<OUT>> output = createOutputCollector(
				containingTask, operatorConfig, chainedConfigs, userCodeClassloader, streamOutputs, allOperators);

		// now create the operator and give it the output collector to write its output to
		OneInputStreamOperator<IN, OUT> chainedOperator = operatorConfig.getStreamOperator(userCodeClassloader);
		chainedOperator.setup(containingTask, operatorConfig, output);

		allOperators.add(chainedOperator);

		if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
			return new ChainingOutput<>(chainedOperator);
		}
		else {
			TypeSerializer<IN> inSerializer = operatorConfig.getTypeSerializerIn1(userCodeClassloader);
			return new CopyingChainingOutput<>(chainedOperator, inSerializer);
		}
	}
	
	private static <T> RecordWriterOutput<T> createStreamOutput(
			StreamEdge edge, StreamConfig upStreamConfig, int outputIndex,
			Environment taskEnvironment,
			String taskName)
	{
		TypeSerializer<T> outSerializer = upStreamConfig.getTypeSerializerOut(taskEnvironment.getUserClassLoader());

		@SuppressWarnings("unchecked")
		StreamPartitioner<T> outputPartitioner = (StreamPartitioner<T>) edge.getPartitioner();

		LOG.debug("Using partitioner {} for output {} of task ", outputPartitioner, outputIndex, taskName);
		
		ResultPartitionWriter bufferWriter = taskEnvironment.getWriter(outputIndex);

		// we initialize the partitioner here with the number of key groups (aka max. parallelism)
		if (outputPartitioner instanceof ConfigurableStreamPartitioner) {
			int numKeyGroups = bufferWriter.getNumTargetKeyGroups();
			if (0 < numKeyGroups) {
				((ConfigurableStreamPartitioner) outputPartitioner).configure(numKeyGroups);
			}
		}

		StreamRecordWriter<SerializationDelegate<StreamRecord<T>>> output = 
				new StreamRecordWriter<>(bufferWriter, outputPartitioner, upStreamConfig.getBufferTimeout());
		output.setMetricGroup(taskEnvironment.getMetricGroup().getIOMetricGroup());
		
		return new RecordWriterOutput<>(output, outSerializer);
	}
	
	// ------------------------------------------------------------------------
	//  Collectors for output chaining
	// ------------------------------------------------------------------------ 

	private static class ChainingOutput<T> implements Output<StreamRecord<T>> {
		
		protected final OneInputStreamOperator<T, ?> operator;
		protected final Counter numRecordsIn;

		public ChainingOutput(OneInputStreamOperator<T, ?> operator) {
			this.operator = operator;
			this.numRecordsIn = ((OperatorMetricGroup) operator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
		}

		@Override
		public void collect(StreamRecord<T> record) {
			try {
				numRecordsIn.inc();
				operator.setKeyContextElement1(record);
				operator.processElement(record);
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}

		@Override
		public void emitWatermark(Watermark mark) {
			try {
				operator.processWatermark(mark);
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {
			try {
				operator.processLatencyMarker(latencyMarker);
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}

		@Override
		public void close() {
			try {
				operator.close();
			}
			catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}
		}
	}

	private static final class CopyingChainingOutput<T> extends ChainingOutput<T> {
		
		private final TypeSerializer<T> serializer;
		
		public CopyingChainingOutput(OneInputStreamOperator<T, ?> operator, TypeSerializer<T> serializer) {
			super(operator);
			this.serializer = serializer;
		}

		@Override
		public void collect(StreamRecord<T> record) {
			try {
				numRecordsIn.inc();
				StreamRecord<T> copy = record.copy(serializer.copy(record.getValue()));
				operator.setKeyContextElement1(copy);
				operator.processElement(copy);
			}
			catch (Exception e) {
				throw new RuntimeException("Could not forward element to next operator", e);
			}
		}
	}
	
	private static class BroadcastingOutputCollector<T> implements Output<StreamRecord<T>> {
		
		protected final Output<StreamRecord<T>>[] outputs;

		private final Random RNG = new XORShiftRandom();
		
		public BroadcastingOutputCollector(Output<StreamRecord<T>>[] outputs) {
			this.outputs = outputs;
		}

		@Override
		public void emitWatermark(Watermark mark) {
			for (Output<StreamRecord<T>> output : outputs) {
				output.emitWatermark(mark);
			}
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {
			if(outputs.length <= 0) {
				// ignore
			} else if(outputs.length == 1) {
				outputs[0].emitLatencyMarker(latencyMarker);
			} else {
				// randomly select an output
				outputs[RNG.nextInt(outputs.length)].emitLatencyMarker(latencyMarker);
			}
		}

		@Override
		public void collect(StreamRecord<T> record) {
			for (Output<StreamRecord<T>> output : outputs) {
				output.collect(record);
			}
		}

		@Override
		public void close() {
			for (Output<StreamRecord<T>> output : outputs) {
				output.close();
			}
		}
	}

	/**
	 * Special version of {@link BroadcastingOutputCollector} that performs a shallow copy of the
	 * {@link StreamRecord} to ensure that multi-chaining works correctly.
	 */
	private static final class CopyingBroadcastingOutputCollector<T> extends BroadcastingOutputCollector<T> {

		public CopyingBroadcastingOutputCollector(Output<StreamRecord<T>>[] outputs) {
			super(outputs);
		}

		@Override
		public void collect(StreamRecord<T> record) {

			for (int i = 0; i < outputs.length - 1; i++) {
				Output<StreamRecord<T>> output = outputs[i];
				StreamRecord<T> shallowCopy = record.copy(record.getValue());
				output.collect(shallowCopy);
			}

			// don't copy for the last output
			outputs[outputs.length - 1].collect(record);
		}
	}
}
