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
package org.apache.flink.streaming.connectors.kinesis;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.config.KinesisConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.examples.ProduceIntoKinesis;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisSerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.nio.ByteBuffer;
import java.util.Properties;

/**
 * This is a manual test for the AWS Kinesis connector in Flink.
 *
 * It uses:
 *  - A custom KinesisSerializationSchema
 *  - A custom KinesisPartitioner
 *
 * Invocation:
 * --region eu-central-1 --accessKey XXXXXXXXXXXX --secretKey XXXXXXXXXXXXXXXX
 */
public class ManualConsumerProducerTest {

	public static void main(String[] args) throws Exception {
		ParameterTool pt = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(4);

		DataStream<String> simpleStringStream = see.addSource(new ProduceIntoKinesis.EventsGenerator());

		FlinkKinesisProducer<String> kinesis = new FlinkKinesisProducer<>(pt.getRequired("region"),
				pt.getRequired("accessKey"),
				pt.getRequired("secretKey"),
				new KinesisSerializationSchema<String>() {
					@Override
					public ByteBuffer serialize(String element) {
						return ByteBuffer.wrap(element.getBytes());
					}

					// every 10th element goes into a different stream
					@Override
					public String getTargetStream(String element) {
						if(element.split("-")[0].endsWith("0")) {
							return "flink-test-2";
						}
						return null; // send to default stream
					}
				});

		kinesis.setFailOnError(true);
		kinesis.setDefaultStream("test-flink");
		kinesis.setDefaultPartition("0");
		kinesis.setCustomPartitioner(new KinesisPartitioner<String>() {
			@Override
			public String getPartitionId(String element) {
				int l = element.length();
				return element.substring(l - 1, l);
			}
		});
		simpleStringStream.addSink(kinesis);


		// consuming topology
		Properties consumerProps = new Properties();
		consumerProps.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_ACCESSKEYID, pt.getRequired("accessKey"));
		consumerProps.setProperty(KinesisConfigConstants.CONFIG_AWS_CREDENTIALS_PROVIDER_BASIC_SECRETKEY, pt.getRequired("secretKey"));
		consumerProps.setProperty(KinesisConfigConstants.CONFIG_AWS_REGION, pt.getRequired("region"));
		DataStream<String> consuming = see.addSource(new FlinkKinesisConsumer<>("test-flink", new SimpleStringSchema(), consumerProps));
		// validate consumed records for correctness
		consuming.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String value, Collector<String> out) throws Exception {
				String[] parts = value.split("-");
				try {
					long l = Long.parseLong(parts[0]);
					if(l < 0) {
						throw new RuntimeException("Negative");
					}
				} catch(NumberFormatException nfe) {
					throw new RuntimeException("First part of '" + value + "' is not a valid numeric type");
				}
				if(parts[1].length() != 12) {
					throw new RuntimeException("Second part of '" + value + "' doesn't have 12 characters");
				}
			}
		});
		consuming.print();

		see.execute();
	}
}
