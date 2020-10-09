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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BoundedData.BoundedPartitionData;
import org.apache.flink.runtime.io.network.partition.BoundedData.BoundedPartitionFileRegion;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * Putting and getting of a sequence of buffers to/from a FileChannel or a ByteBuffer.
 * This class handles the headers, length encoding, memory slicing.
 *
 * <p>The encoding is the same across FileChannel and ByteBuffer, so this class can
 * write to a file and read from the byte buffer that results from mapping this file to memory.
 */
final class BufferReaderWriterUtil {

	static final int HEADER_LENGTH = 8;

	private static final short HEADER_VALUE_IS_BUFFER = 0;

	private static final short HEADER_VALUE_IS_EVENT = 1;

	private static final short BUFFER_IS_COMPRESSED = 1;

	private static final short BUFFER_IS_NOT_COMPRESSED = 0;

	// ------------------------------------------------------------------------
	//  ByteBuffer read / write
	// ------------------------------------------------------------------------

	static boolean writeBuffer(Buffer buffer, ByteBuffer memory) {
		final int bufferSize = buffer.getSize();

		if (memory.remaining() < bufferSize + HEADER_LENGTH) {
			return false;
		}

		memory.putShort(buffer.isBuffer() ? HEADER_VALUE_IS_BUFFER : HEADER_VALUE_IS_EVENT);
		memory.putShort(buffer.isCompressed() ? BUFFER_IS_COMPRESSED : BUFFER_IS_NOT_COMPRESSED);
		memory.putInt(bufferSize);
		memory.put(buffer.getNioBufferReadable());
		return true;
	}

	@Nullable
	static Buffer sliceNextBuffer(ByteBuffer memory) {
		final int remaining = memory.remaining();

		// we only check the correct case where data is exhausted
		// all other cases can only occur if our write logic is wrong and will already throw
		// buffer underflow exceptions which will cause the read to fail.
		if (remaining == 0) {
			return null;
		}

		final boolean isEvent = memory.getShort() == HEADER_VALUE_IS_EVENT;
		final boolean isCompressed = memory.getShort() == BUFFER_IS_COMPRESSED;
		final int size = memory.getInt();

		memory.limit(memory.position() + size);
		ByteBuffer buf = memory.slice();
		memory.position(memory.limit());
		memory.limit(memory.capacity());

		MemorySegment memorySegment = MemorySegmentFactory.wrapOffHeapMemory(buf);

		Buffer.DataType dataType = isEvent ? Buffer.DataType.EVENT_BUFFER : Buffer.DataType.DATA_BUFFER;
		return new NetworkBuffer(memorySegment, FreeingBufferRecycler.INSTANCE, dataType, isCompressed, size);
	}

	// ------------------------------------------------------------------------
	//  ByteChannel read / write
	// ------------------------------------------------------------------------

	static long writeToByteChannel(
			FileChannel channel,
			Buffer buffer,
			ByteBuffer[] arrayWithHeaderBuffer) throws IOException {

		final ByteBuffer headerBuffer = arrayWithHeaderBuffer[0];
		headerBuffer.clear();
		headerBuffer.putShort(buffer.isBuffer() ? HEADER_VALUE_IS_BUFFER : HEADER_VALUE_IS_EVENT);
		headerBuffer.putShort(buffer.isCompressed() ? BUFFER_IS_COMPRESSED : BUFFER_IS_NOT_COMPRESSED);
		headerBuffer.putInt(buffer.getSize());
		headerBuffer.flip();

		final ByteBuffer dataBuffer = buffer.getNioBufferReadable();
		arrayWithHeaderBuffer[1] = dataBuffer;

		final long bytesExpected = HEADER_LENGTH + dataBuffer.remaining();

		// The file channel implementation guarantees that all bytes are written when invoked
		// because it is a blocking channel (the implementation mentioned it as guaranteed).
		// However, the api docs leaves it somewhat open, so it seems to be an undocumented contract in the JRE.
		// We build this safety net to be on the safe side.
		if (bytesExpected < channel.write(arrayWithHeaderBuffer)) {
			writeBuffers(channel, arrayWithHeaderBuffer);
		}
		return bytesExpected;
	}

	static long writeToByteChannelIfBelowSize(
			FileChannel channel,
			Buffer buffer,
			ByteBuffer[] arrayWithHeaderBuffer,
			long bytesLeft) throws IOException {

		if (bytesLeft >= HEADER_LENGTH + buffer.getSize()) {
			return writeToByteChannel(channel, buffer, arrayWithHeaderBuffer);
		}

		return -1L;
	}

	@Nullable
	static BoundedPartitionData readFromByteChannel(FileChannel channel, ByteBuffer headerBuffer) throws IOException {

		headerBuffer.clear();
		if (!tryReadByteBuffer(channel, headerBuffer)) {
			return null;
		}
		headerBuffer.flip();

		boolean isEvent = headerBuffer.getShort() == HEADER_VALUE_IS_EVENT;
		Buffer.DataType dataType = isEvent ? Buffer.DataType.EVENT_BUFFER : Buffer.DataType.DATA_BUFFER;
		boolean isCompressed = headerBuffer.getShort() == BUFFER_IS_COMPRESSED;
		int dataSize = headerBuffer.getInt();

		return new BoundedPartitionFileRegion(channel, dataSize, dataType, isCompressed);
	}

	static ByteBuffer allocatedHeaderBuffer() {
		ByteBuffer bb = ByteBuffer.allocateDirect(HEADER_LENGTH);
		configureByteBuffer(bb);
		return bb;
	}

	static ByteBuffer[] allocatedWriteBufferArray() {
		return new ByteBuffer[] { allocatedHeaderBuffer(), null };
	}

	private static boolean tryReadByteBuffer(FileChannel channel, ByteBuffer b) throws IOException {
		if (channel.read(b) == -1) {
			return false;
		}
		else {
			while (b.hasRemaining()) {
				if (channel.read(b) == -1) {
					throwPrematureEndOfFile();
				}
			}
			return true;
		}
	}

	static void readByteBufferFully(FileChannel channel, ByteBuffer b) throws IOException {
		// the post-checked loop here gets away with one less check in the normal case
		do {
			if (channel.read(b) == -1) {
				throwPrematureEndOfFile();
			}
		}
		while (b.hasRemaining());
	}

	private static void writeBuffer(FileChannel channel, ByteBuffer buffer) throws IOException {
		while (buffer.hasRemaining()) {
			channel.write(buffer);
		}
	}

	private static void writeBuffers(FileChannel channel, ByteBuffer... buffers) throws IOException {
		for (ByteBuffer buffer : buffers) {
			writeBuffer(channel, buffer);
		}
	}

	private static void throwPrematureEndOfFile() throws IOException {
		throw new IOException("The spill file is corrupt: premature end of file");
	}

	// ------------------------------------------------------------------------
	//  Utils
	// ------------------------------------------------------------------------

	static void configureByteBuffer(ByteBuffer buffer) {
		buffer.order(ByteOrder.nativeOrder());
	}
}
