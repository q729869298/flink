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

package org.apache.flink.docs.rest;

import org.apache.flink.docs.rest.data.TestDocumentingRestEndpoint;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * Tests for the {@link RestAPIDocGenerator}.
 */
public class RestAPIDocGeneratorTest extends TestLogger {

	@Test
	public void testExcludeFromDocumentation() throws Exception {
		File file = File.createTempFile("rest_v0_", ".html");
		RestAPIDocGenerator.createHtmlFile(
			new TestDocumentingRestEndpoint(),
			RestAPIVersion.V0,
			file.toPath());
		String actual = FileUtils.readFile(file, "UTF-8");

		Assert.assertTrue(actual.contains("This is a testing REST API."));
		Assert.assertTrue(actual.contains("This is an empty testing REST API."));
		Assert.assertFalse(actual.contains("This REST API should not appear in the generated documentation."));
	}
}
