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

import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.WriteBufferManager;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests to guard {@link RocksDBResourceContainer}.
 */
public class RocksDBResourceContainerTest {

	@ClassRule
	public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

	@BeforeClass
	public static void ensureRocksDbNativeLibraryLoaded() throws IOException {
		NativeLibraryLoader.getInstance().loadLibrary(TMP_FOLDER.newFolder().getAbsolutePath());
	}

	// ------------------------------------------------------------------------

	@Test
	public void testFreeDBOptionsAfterClose() throws Exception {
		RocksDBResourceContainer container = new RocksDBResourceContainer();
		DBOptions dbOptions = container.getDbOptions();
		assertThat(dbOptions.isOwningHandle(), is(true));
		container.close();
		assertThat(dbOptions.isOwningHandle(), is(false));
	}

	@Test
	public void testFreeMultipleDBOptionsAfterClose() throws Exception {
		RocksDBResourceContainer container = new RocksDBResourceContainer();
		final int optionNumber = 20;
		ArrayList<DBOptions> dbOptions = new ArrayList<>(optionNumber);
		for (int i = 0; i < optionNumber; i++) {
			dbOptions.add(container.getDbOptions());
		}
		container.close();
		for (DBOptions dbOption: dbOptions) {
			assertThat(dbOption.isOwningHandle(), is(false));
		}
	}

	@Test
	public void testSharedResources() throws Exception {
		final int optionNumber = 20;
		final long cacheSize = 1024L, writeBufferSize = 512L;
		final LRUCache cache = new LRUCache(cacheSize, -1, false, 0.1);
		final WriteBufferManager wbm = new WriteBufferManager(writeBufferSize, cache);
		RocksDBSharedResources rocksDBSharedResources = new RocksDBSharedResources(cache, wbm);
		OpaqueMemoryResource<RocksDBSharedResources> sharedResources =
			new OpaqueMemoryResource<>(rocksDBSharedResources, cacheSize, rocksDBSharedResources::close);
		RocksDBResourceContainer container =
			new RocksDBResourceContainer(PredefinedOptions.DEFAULT, null, sharedResources);
		HashSet<WriteBufferManager> writeBufferManagers = new HashSet<>();
		for (int i = 0; i < optionNumber; i++) {
			DBOptions dbOptions = container.getDbOptions();
			WriteBufferManager writeBufferManager = getWriteBufferManager(dbOptions);
			writeBufferManagers.add(writeBufferManager);
		}
		assertThat(writeBufferManagers.size(), is(1));
		assertThat(writeBufferManagers.iterator().next(), is(rocksDBSharedResources.getWriteBufferManager()));
		container.close();
		assertThat(rocksDBSharedResources.getCache().isOwningHandle(), is(false));
		assertThat(rocksDBSharedResources.getWriteBufferManager().isOwningHandle(), is(false));
	}

	private WriteBufferManager getWriteBufferManager(DBOptions dbOptions) {

		Field writeBufferManagerField = null;
		try {
			writeBufferManagerField = DBOptions.class.getDeclaredField("writeBufferManager_");
		} catch (NoSuchFieldException e) {
			fail("writeBufferManager_ is not defined.");
		}
		writeBufferManagerField.setAccessible(true);
		try {
			return (WriteBufferManager) writeBufferManagerField.get(dbOptions);
		} catch (IllegalAccessException e) {
			fail("Cannot access writeBufferManager_ field.");
			return null;
		}
	}

	@Test
	public void testFreeColumnOptionsAfterClose() throws Exception {
		RocksDBResourceContainer container = new RocksDBResourceContainer();
		ColumnFamilyOptions columnFamilyOptions = container.getColumnOptions();
		assertThat(columnFamilyOptions.isOwningHandle(), is(true));
		container.close();
		assertThat(columnFamilyOptions.isOwningHandle(), is(false));
	}

	@Test
	public void testFreeMultipleColumnOptionsAfterClose() throws Exception {
		RocksDBResourceContainer container = new RocksDBResourceContainer();
		final int optionNumber = 20;
		ArrayList<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(optionNumber);
		for (int i = 0; i < optionNumber; i++) {
			columnFamilyOptions.add(container.getColumnOptions());
		}
		container.close();
		for (ColumnFamilyOptions columnFamilyOption: columnFamilyOptions) {
			assertThat(columnFamilyOption.isOwningHandle(), is(false));
		}
	}

	@Test
	public void testFreeMultipleColumnOptionsWithPredefinedOptions() throws Exception {
		for (PredefinedOptions predefinedOptions: PredefinedOptions.values()) {
			RocksDBResourceContainer container = new RocksDBResourceContainer(predefinedOptions, null);
			final int optionNumber = 20;
			ArrayList<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(optionNumber);
			for (int i = 0; i < optionNumber; i++) {
				columnFamilyOptions.add(container.getColumnOptions());
			}
			container.close();
			for (ColumnFamilyOptions columnFamilyOption: columnFamilyOptions) {
				assertThat(columnFamilyOption.isOwningHandle(), is(false));
			}
		}
	}

	@Test
	public void testFreeSharedResourcesAfterClose() throws Exception {
		LRUCache cache = new LRUCache(1024L);
		WriteBufferManager wbm = new WriteBufferManager(1024L, cache);
		RocksDBSharedResources sharedResources = new RocksDBSharedResources(cache, wbm);
		final ThrowingRunnable<Exception> disposer = sharedResources::close;
		OpaqueMemoryResource<RocksDBSharedResources> opaqueResource =
			new OpaqueMemoryResource<>(sharedResources, 1024L, disposer);

		RocksDBResourceContainer container = new RocksDBResourceContainer(PredefinedOptions.DEFAULT, null, opaqueResource);

		container.close();
		assertThat(cache.isOwningHandle(), is(false));
		assertThat(wbm.isOwningHandle(), is(false));
	}
}
