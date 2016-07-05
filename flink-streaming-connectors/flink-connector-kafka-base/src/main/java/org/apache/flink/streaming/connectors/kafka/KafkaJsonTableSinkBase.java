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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.Row;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.AvroSerializationSchema;
import org.apache.flink.streaming.util.serialization.JsonRowSerializationSchema;

import java.util.Properties;

/**
 * Base class for {@link KafkaTableSink} that serializes data in JSON format
 */
public abstract class KafkaJsonTableSinkBase extends KafkaTableSink {

	/**
	 * Creates KafkaJsonTableSinkBase
	 *
	 * @param topic topic in Kafka
	 * @param properties properties to connect to Kafka
	 * @param partitioner Kafra partitioner
	 * @param fieldNames row field names
	 * @param fieldTypes row field types
	 */
	public KafkaJsonTableSinkBase(String topic, Properties properties, KafkaPartitioner<Row> partitioner, String[] fieldNames, Class<?>[] fieldTypes) {
		super(topic, properties, new JsonRowSerializationSchema(fieldNames), partitioner, fieldNames, fieldTypes);
	}

	/**
	 * Creates KafkaJsonTableSinkBase
	 *
	 * @param topic topic in Kafka
	 * @param properties properties to connect to Kafka
	 * @param partitioner Kafra partitioner
	 * @param fieldNames row field names
	 * @param fieldTypes row field types
	 */
	public KafkaJsonTableSinkBase(String topic, Properties properties, KafkaPartitioner<Row> partitioner, String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		super(topic, properties, new JsonRowSerializationSchema(fieldNames), partitioner, fieldNames, fieldTypes);
	}

}
