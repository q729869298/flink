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
package org.apache.flink.streaming.connectors.kafka;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class FlinkKafkaConsumer08WithPeriodicWM<T> extends FlinkKafkaConsumer08Base<T> implements Triggerable {

	/**
	 * The interval between periodic watermark emissions, as configured via the
	 * {@link ExecutionConfig#getAutoWatermarkInterval()}.
	 */
	private long watermarkInterval = -1;

	private StreamingRuntimeContext runtime = null;

	private SourceContext<T> srcContext = null;

	/**
	 * The user-specified methods to extract the timestamps from the records in Kafka, and
	 * to decide when to emit watermarks.
	 */
	private final AssignerWithPeriodicWatermarks<T> periodicWatermarkAssigner;

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.8.x
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param valueDeserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 * @param timestampAssigner
	 *           The user-specified methods to extract the timestamps and decide when to emit watermarks.
	 *           This has to implement the {@link AssignerWithPeriodicWatermarks} interface.
	 */
	public FlinkKafkaConsumer08WithPeriodicWM(String topic,
												DeserializationSchema<T> valueDeserializer,
												Properties props,
												AssignerWithPeriodicWatermarks<T> timestampAssigner) {
		this(Collections.singletonList(topic), valueDeserializer, props, timestampAssigner);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.8.x
	 *
	 * This constructor allows passing a {@see KeyedDeserializationSchema} for reading key/value
	 * pairs, offsets, and topic names from Kafka.
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 * @param timestampAssigner
	 *           The user-specified methods to extract the timestamps and decide when to emit watermarks.
	 *           This has to implement the {@link AssignerWithPeriodicWatermarks} interface.
	 */
	public FlinkKafkaConsumer08WithPeriodicWM(String topic,
												KeyedDeserializationSchema<T> deserializer,
												Properties props,
												AssignerWithPeriodicWatermarks<T> timestampAssigner) {
		this(Collections.singletonList(topic), deserializer, props, timestampAssigner);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.8.x
	 *
	 * This constructor allows passing multiple topics to the consumer.
	 *
	 * @param topics
	 *           The Kafka topics to read from.
	 * @param deserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 * @param timestampAssigner
	 *           The user-specified methods to extract the timestamps and decide when to emit watermarks.
	 *           This has to implement the {@link AssignerWithPeriodicWatermarks} interface.
	 */
	public FlinkKafkaConsumer08WithPeriodicWM(List<String> topics,
												DeserializationSchema<T> deserializer,
												Properties props,
												AssignerWithPeriodicWatermarks<T> timestampAssigner) {
		this(topics, new KeyedDeserializationSchemaWrapper<>(deserializer), props, timestampAssigner);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.8.x
	 *
	 * This constructor allows passing multiple topics and a key/value deserialization schema.
	 *
	 * @param topics
	 *           The Kafka topics to read from.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 * @param timestampAssigner
	 *           The user-specified methods to extract the timestamps and decide when to emit watermarks.
	 *           This has to implement the {@link AssignerWithPeriodicWatermarks} interface.
	 */
	public FlinkKafkaConsumer08WithPeriodicWM(List<String> topics,
												KeyedDeserializationSchema<T> deserializer,
												Properties props,
												AssignerWithPeriodicWatermarks<T> timestampAssigner) {
		super(topics, deserializer, props);
		this.periodicWatermarkAssigner = Preconditions.checkNotNull(timestampAssigner);
	}

	@Override
	public void processElement(SourceFunction.SourceContext<T> sourceContext, String topic, int partition, T value) {
		if(srcContext == null) {
			srcContext = sourceContext;
		}

		// extract the timestamp based on the user-specified extractor
		// emits the element with the new timestamp
		// updates the list of minimum timestamps seen per topic per partition (if necessary)

		long extractedTimestamp = periodicWatermarkAssigner.extractTimestamp(value, Long.MIN_VALUE);
		sourceContext.collectWithTimestamp(value, extractedTimestamp);
		updateMaximumTimestampForPartition(topic, partition, extractedTimestamp);
	}

	@Override
	public void trigger(long timestamp) throws Exception {
		if(srcContext == null) {
			throw new RuntimeException("The source context has not been initialized.");
		}

		// get the minimum seen timestamp across ALL topics AND partitions
		// and send it in the stream.
		emitWatermarkIfMarkingProgress(srcContext);
		setNextWatermarkTimer(runtime);
	}

	private void setNextWatermarkTimer(StreamingRuntimeContext runtime) {
		long timeToNextWatermark = getTimeToNextWaternark();
		runtime.registerTimer(timeToNextWatermark, this);
	}

	private long getTimeToNextWaternark() {
		return System.currentTimeMillis() + watermarkInterval;
	}

}
