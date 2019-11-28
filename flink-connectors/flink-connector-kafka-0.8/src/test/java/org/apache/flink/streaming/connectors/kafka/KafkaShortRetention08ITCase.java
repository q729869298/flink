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

import org.apache.flink.testutils.junit.FailsOnJava11;

import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * {@link KafkaShortRetentionTestBase} for Kafka 0.8 .
 */
@SuppressWarnings("serial")
public class KafkaShortRetention08ITCase extends KafkaShortRetentionTestBase {

	@Test(timeout = 60000)
	@Category(FailsOnJava11.class)
	public void testAutoOffsetReset() throws Exception {
		runAutoOffsetResetTest();
	}

	@Test(timeout = 60000)
	public void testAutoOffsetResetNone() throws Exception {
		runFailOnAutoOffsetResetNoneEager();
	}
}
