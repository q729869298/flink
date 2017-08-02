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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for the JobStoppingHandler.
 */
public class JobStoppingHandlerTest extends TestLogger {
	@Test
	public void testGetPaths() {
		JobStoppingHandler handler = new JobStoppingHandler(TestingUtils.TIMEOUT());
		String[] paths = handler.getPaths();
		Assert.assertEquals(2, paths.length);
		List<String> pathsList = Lists.newArrayList(paths);
		Assert.assertTrue(pathsList.contains("/jobs/:jobid/stop"));
		Assert.assertTrue(pathsList.contains("/jobs/:jobid/yarn-stop"));
	}
}
