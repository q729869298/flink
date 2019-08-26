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

package org.apache.flink.metrics.prometheus;

import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Test for {@link PrometheusPushGatewayReporter}.
 */
public class PrometheusPushGatewayReporterTest extends TestLogger {

	@Test
	public void testParserGroupingKey() {
		PrometheusPushGatewayReporter reporter = new PrometheusPushGatewayReporter();
		Map<String, String> groupingKey = reporter.parseGroupingKey("k1=v1;k2=v2");
		Assert.assertNotNull(groupingKey);
		Assert.assertEquals("v1", groupingKey.get("k1"));
		Assert.assertEquals("v2", groupingKey.get("k2"));
	}

	@Test
	public void testParserIncompleteGroupingKey() {
		PrometheusPushGatewayReporter reporter = new PrometheusPushGatewayReporter();
		Map<String, String> groupingKey = reporter.parseGroupingKey("k1=");
		Assert.assertNotNull(groupingKey);
		Assert.assertEquals("", groupingKey.get("k1"));

		groupingKey = reporter.parseGroupingKey("=v1");
		Assert.assertNotNull(groupingKey);
		Assert.assertEquals("v1", groupingKey.get(""));

		groupingKey = reporter.parseGroupingKey("k1");
		Assert.assertNotNull(groupingKey);
		Assert.assertTrue(groupingKey.isEmpty());
	}
}
