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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.Optional;
import java.util.Properties;

/**
 * Kafka 0.11 table sink for writing data into Kafka.
 */
@Internal
public class Kafka011TableSink extends KafkaTableSink {

	/**
	 * Creates a Kafka 0.11 table sink.
	 *
	 * @param schema              The schema of the table.
	 * @param topic               Kafka topic to write to.
	 * @param properties          Properties for the Kafka producer.
	 * @param partitioner         Partitioner to select Kafka partition for each item.
	 * @param serializationSchema Serialization schema for encoding records to Kafka.
	 */
	public Kafka011TableSink(
			TableSchema schema,
			String topic,
			Properties properties,
			FlinkKafkaPartitioner<Row> partitioner,
			SerializationSchema<Row> serializationSchema) {
		super(
			schema,
			topic,
			properties,
			partitioner,
			serializationSchema);
	}

	@Override
	protected SinkFunction<Row> createKafkaProducer(
			String topic,
			Properties properties,
			SerializationSchema<Row> serializationSchema,
			FlinkKafkaPartitioner<Row> partitioner) {
		return new FlinkKafkaProducer011<>(
			topic,
			new KeyedSerializationSchemaWrapper<>(serializationSchema),
			properties,
			Optional.of(partitioner));
	}
}
