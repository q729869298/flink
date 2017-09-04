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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemoryType;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.IsNot.not;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NetworkBufferPoolTest {

	@Test
	public void testCreatePoolAfterDestroy() {
		try {
			final int bufferSize = 128;
			final int numBuffers = 10;

			NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, bufferSize, MemoryType.HEAP);
			assertEquals(bufferSize, globalPool.getMemorySegmentSize());
			assertEquals(numBuffers, globalPool.getTotalNumberOfMemorySegments());
			assertEquals(numBuffers, globalPool.getNumberOfAvailableMemorySegments());
			assertEquals(0, globalPool.getNumberOfRegisteredBufferPools());

			globalPool.destroy();

			assertTrue(globalPool.isDestroyed());

			try {
				globalPool.createBufferPool(2, 2);
				fail("Should throw an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// yippie!
			}

			try {
				globalPool.createBufferPool(2, 10);
				fail("Should throw an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// yippie!
			}

			try {
				globalPool.createBufferPool(2, Integer.MAX_VALUE);
				fail("Should throw an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// yippie!
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

	}

	@Test
	public void testDestroyAll() {
		try {
			NetworkBufferPool globalPool = new NetworkBufferPool(10, 128, MemoryType.HEAP);

			BufferPool fixedPool = globalPool.createBufferPool(2, 2);
			BufferPool boundedPool = globalPool.createBufferPool(0, 1);
			BufferPool nonFixedPool = globalPool.createBufferPool(5, Integer.MAX_VALUE);

			assertEquals(2, fixedPool.getNumberOfRequiredMemorySegments());
			assertEquals(0, boundedPool.getNumberOfRequiredMemorySegments());
			assertEquals(5, nonFixedPool.getNumberOfRequiredMemorySegments());

			// actually, the buffer pool sizes may be different due to rounding and based on the internal order of
			// the buffer pools - the total number of retrievable buffers should be equal to the number of buffers
			// in the NetworkBufferPool though

			ArrayList<Buffer> buffers = new ArrayList<>(globalPool.getTotalNumberOfMemorySegments());
			collectBuffers:
			for (int i = 0; i < 10; ++i) {
				for (BufferPool bp : new BufferPool[] { fixedPool, boundedPool, nonFixedPool }) {
					Buffer buffer = bp.requestBuffer();
					if (buffer != null) {
						assertNotNull(buffer.getMemorySegment());
						buffers.add(buffer);
						continue collectBuffers;
					}
				}
			}

			assertEquals(globalPool.getTotalNumberOfMemorySegments(), buffers.size());

			assertNull(fixedPool.requestBuffer());
			assertNull(boundedPool.requestBuffer());
			assertNull(nonFixedPool.requestBuffer());

			// destroy all allocated ones
			globalPool.destroyAllBufferPools();

			// check the destroyed status
			assertFalse(globalPool.isDestroyed());
			assertTrue(fixedPool.isDestroyed());
			assertTrue(boundedPool.isDestroyed());
			assertTrue(nonFixedPool.isDestroyed());

			assertEquals(0, globalPool.getNumberOfRegisteredBufferPools());

			// buffers are not yet recycled
			assertEquals(0, globalPool.getNumberOfAvailableMemorySegments());

			// the recycled buffers should go to the global pool
			for (Buffer b : buffers) {
				b.recycle();
			}
			assertEquals(globalPool.getTotalNumberOfMemorySegments(), globalPool.getNumberOfAvailableMemorySegments());

			// can request no more buffers
			try {
				fixedPool.requestBuffer();
				fail("Should fail with an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// yippie!
			}

			try {
				boundedPool.requestBuffer();
				fail("Should fail with an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// that's the way we like it, aha, aha
			}

			try {
				nonFixedPool.requestBuffer();
				fail("Should fail with an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// stayin' alive
			}

			// can create a new pool now
			assertNotNull(globalPool.createBufferPool(10, Integer.MAX_VALUE));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Tests {@link NetworkBufferPool#requestMemorySegments(int)} with the {@link NetworkBufferPool}
	 * currently containing the number of required free segments.
	 */
	@Test
	public void testRequestMemorySegmentsLessThanTotalBuffers() throws Exception {
		final int numBuffers = 10;

		NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128, MemoryType.HEAP);

		List<MemorySegment> memorySegments = Collections.emptyList();
		try {
			memorySegments = globalPool.requestMemorySegments(numBuffers / 2);

			assertEquals(memorySegments.size(), numBuffers / 2);
		} finally {
			globalPool.recycleMemorySegments(memorySegments);
			assertEquals(globalPool.getNumberOfAvailableMemorySegments(), numBuffers);
		}
	}

	/**
	 * Tests {@link NetworkBufferPool#requestMemorySegments(int)} with the number of required
	 * buffers exceeding the capacity of {@link NetworkBufferPool}.
	 */
	@Test
	public void testRequestMemorySegmentsMoreThanTotalBuffers() throws Exception {
		final int numBuffers = 10;

		NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128, MemoryType.HEAP);

		List<MemorySegment> memorySegments = Collections.emptyList();
		try {
			memorySegments = globalPool.requestMemorySegments(numBuffers + 1);
			fail("Should throw an IOException");
		} catch (IOException e) {
			assertEquals(memorySegments.size(), 0);
			assertEquals(globalPool.getNumberOfAvailableMemorySegments(), numBuffers);
		}
	}

	/**
	 * Tests {@link NetworkBufferPool#requestMemorySegments(int)} with the invalid argument to
	 * cause exception.
	 */
	@Test
	public void testRequestMemorySegmentsWithInvalidArgument() throws Exception {
		final int numBuffers = 10;

		NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128, MemoryType.HEAP);

		List<MemorySegment> memorySegments = Collections.emptyList();
		try {
			// the number of requested buffers should be larger than zero
			memorySegments = globalPool.requestMemorySegments(0);
			fail("Should throw an IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			assertEquals(memorySegments.size(), 0);
			assertEquals(globalPool.getNumberOfAvailableMemorySegments(), numBuffers);
		}
	}

	/**
	 * Tests {@link NetworkBufferPool#requestMemorySegments(int)} with the {@link NetworkBufferPool}
	 * currently not containing the number of required free segments (currently occupied by a buffer pool).
	 */
	@Test
	public void testRequestMemorySegmentsWithBuffersTaken() throws IOException, InterruptedException {
		final int numBuffers = 10;

		NetworkBufferPool networkBufferPool = new NetworkBufferPool(numBuffers, 128, MemoryType.HEAP);

		final List<Buffer> buffers = new ArrayList<>(numBuffers);
		List<MemorySegment> memorySegments = Collections.emptyList();
		Thread bufferRecycler = null;
		BufferPool lbp1 = null;
		try {
			lbp1 = networkBufferPool.createBufferPool(numBuffers / 2, numBuffers);

			// take all buffers (more than the minimum required)
			for (int i = 0; i < numBuffers; ++i) {
				Buffer buffer = lbp1.requestBuffer();
				buffers.add(buffer);
				assertNotNull(buffer);
			}

			// if requestMemorySegments() blocks, this will make sure that enough buffers are freed
			// eventually for it to continue
			bufferRecycler = new Thread(() -> {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException ignored) {
				}

				for (Buffer buffer : buffers) {
					buffer.recycle();
				}
			});
			bufferRecycler.start();

			// take more buffers than are freely available at the moment via requestMemorySegments()
			memorySegments = networkBufferPool.requestMemorySegments(numBuffers / 2);
			assertThat(memorySegments, not(hasItem(nullValue())));
		} finally {
			if (bufferRecycler != null) {
				bufferRecycler.join();
			}
			if (lbp1 != null) {
				lbp1.lazyDestroy();
			}
			networkBufferPool.recycleMemorySegments(memorySegments);
		}
	}
}
