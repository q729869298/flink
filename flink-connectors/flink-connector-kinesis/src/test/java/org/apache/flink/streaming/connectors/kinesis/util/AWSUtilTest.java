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

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.StartingPosition;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.WebIdentityTokenCredentialsProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import static com.amazonaws.services.kinesis.model.ShardIteratorType.AT_TIMESTAMP;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM;
import static org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber.SENTINEL_LATEST_SEQUENCE_NUM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for AWSUtil.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AWSUtil.class)
public class AWSUtilTest {
	@Rule
	private final ExpectedException exception = ExpectedException.none();

	@Test
	public void testDefaultCredentialsProvider() {
		Properties testConfig = new Properties();

		AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(testConfig);

		assertTrue(credentialsProvider instanceof DefaultAWSCredentialsProviderChain);
	}

	@Test
	public void testGetCredentialsProvider() {
		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "WEB_IDENTITY_TOKEN");

		AWSCredentialsProvider credentialsProvider = AWSUtil.getCredentialsProvider(testConfig);
		assertTrue(credentialsProvider instanceof WebIdentityTokenCredentialsProvider);
	}

	@Test
	public void testInvalidCredentialsProvider() {
		exception.expect(IllegalArgumentException.class);

		Properties testConfig = new Properties();
		testConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "INVALID_PROVIDER");

		AWSUtil.getCredentialsProvider(testConfig);
	}

	@Test
	public void testValidRegion() {
		assertTrue(AWSUtil.isValidRegion("us-east-1"));
	}

	@Test
	public void testInvalidRegion() {
		assertFalse(AWSUtil.isValidRegion("ur-east-1"));
	}

	@Test
	public void testGetStartingPositionForLatest() {
		StartingPosition position = AWSUtil.getStartingPosition(SENTINEL_LATEST_SEQUENCE_NUM.get(), new Properties());

		assertEquals(LATEST, position.getShardIteratorType());
		assertNull(position.getStartingMarker());
	}

	@Test
	public void testGetStartingPositionForTimestamp() throws Exception {
		String timestamp = "2020-08-13T09:18:00.0+01:00";
		Date expectedTimestamp = new SimpleDateFormat(DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT).parse(timestamp);

		Properties consumerProperties = new Properties();
		consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, timestamp);

		StartingPosition position = AWSUtil.getStartingPosition(SENTINEL_AT_TIMESTAMP_SEQUENCE_NUM.get(), consumerProperties);

		assertEquals(AT_TIMESTAMP, position.getShardIteratorType());
		assertEquals(expectedTimestamp, position.getStartingMarker());
	}
}
