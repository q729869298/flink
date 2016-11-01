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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.util.OperatingSystem;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the partitioned state part of {@link RocksDBStateBackend}.
 */
public class RocksDBStateBackendTest extends StateBackendTestBase<RocksDBStateBackend> {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void checkOperatingSystem() {
		Assume.assumeTrue("This test can't run successfully on Windows.", !OperatingSystem.isWindows());
	}

	@Override
	protected RocksDBStateBackend getStateBackend() throws IOException {
		String dbPath = tempFolder.newFolder().getAbsolutePath();
		String checkpointPath = tempFolder.newFolder().toURI().toString();
		RocksDBStateBackend backend = new RocksDBStateBackend(checkpointPath, new FsStateBackend(checkpointPath));
		backend.setDbStoragePath(dbPath);
		return backend;
	}

	/**
	 * Tests that the externalized checkpoint directory is set to the base
	 * checkpoint directory of the checkpoint backend.
	 */
	@Test
	public void testExternalizedCheckpointsDirectoryConfiguration() throws Exception {
		// Default case, use checkpoint directory
		Path expected = FsStateBackend.validateAndNormalizeUri(new Path("file://" + tempFolder.newFolder()).toUri());
		RocksDBStateBackend backend = new RocksDBStateBackend(expected.toUri());
		assertEquals(expected, backend.getCheckpointDirectory());

		// Use the checkpoint backend external directory
		AbstractStateBackend checkpointBackend = mock(AbstractStateBackend.class);
		Path newExpected = new Path(tempFolder.newFolder().getAbsolutePath());
		when(checkpointBackend.getCheckpointDirectory()).thenReturn(newExpected);

		backend = new RocksDBStateBackend(expected.toUri(), checkpointBackend);
		assertEquals(newExpected, backend.getCheckpointDirectory());
	}

}
