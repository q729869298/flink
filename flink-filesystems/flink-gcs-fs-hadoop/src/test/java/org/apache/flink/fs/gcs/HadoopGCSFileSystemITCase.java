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

package org.apache.flink.fs.gcs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.AbstractHadoopFileSystemITTest;

import org.junit.BeforeClass;

import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertFalse;

/**
 *
 */
public class HadoopGCSFileSystemITCase extends AbstractHadoopFileSystemITTest {

	@BeforeClass
	public static void setup() throws IOException {
		// check whether credentials exist
//		S3TestCredentials.assumeCredentialsAvailable();

		// initialize configuration with valid credentials
		final Configuration conf = new Configuration();
//		conf.setString("s3.access.key", S3TestCredentials.getS3AccessKey());
//		conf.setString("s3.secret.key", S3TestCredentials.getS3SecretKey());
		FileSystem.initialize(conf);

		basePath = new Path("gs://temp/tests-" + UUID.randomUUID());
		fs = basePath.getFileSystem();
		deadline = System.nanoTime() + 30_000_000_000L;

		// check for uniqueness of the test directory
		// directory must not yet exist
		assertFalse(fs.exists(basePath));
	}
}
