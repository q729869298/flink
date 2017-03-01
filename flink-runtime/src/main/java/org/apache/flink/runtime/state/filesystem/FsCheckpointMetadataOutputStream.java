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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointMetadataStreamFactory.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointMetadataStreamFactory.StreamHandleAndPointer;
import org.apache.flink.runtime.state.StreamStateHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link CheckpointMetadataOutputStream} that writes into a file and
 * returns a {@link StreamStateHandle} upon closing.
 */
public final class FsCheckpointMetadataOutputStream extends CheckpointMetadataOutputStream {

	private static final Logger LOG = LoggerFactory.getLogger(FsCheckpointMetadataOutputStream.class);

	// ------------------------------------------------------------------------

	private final FSDataOutputStream out;

	private final Path path;

	private final FileSystem fileSystem;

	private volatile boolean closed;


	public FsCheckpointMetadataOutputStream(FileSystem fileSystem, Path path) throws IOException {
		this.fileSystem = checkNotNull(fileSystem);
		this.path = checkNotNull(path);

		this.out = fileSystem.create(path, WriteMode.NO_OVERWRITE);
	}

	// ------------------------------------------------------------------------
	//  I/O
	// ------------------------------------------------------------------------

	@Override
	public final void write(int b) throws IOException {
		out.write(b);
	}

	@Override
	public final void write(byte[] b, int off, int len) throws IOException {
		out.write(b, off, len);
	}

	@Override
	public long getPos() throws IOException {
		return out.getPos();
	}

	@Override
	public void flush() throws IOException {
		out.flush();
	}

	@Override
	public void sync() throws IOException {
		out.sync();
	}

	// ------------------------------------------------------------------------
	//  Closing
	// ------------------------------------------------------------------------

	public boolean isClosed() {
		return closed;
	}

	@Override
	public void close() {
		if (!closed) {
			closed = true;

			try {
				out.close();
				fileSystem.delete(path, false);
			}
			catch (Throwable t) {
				LOG.warn("Could not close the state stream for {}.", path, t);
			}
		}
	}

	@Override
	public StreamHandleAndPointer closeAndGetPointerHandle() throws IOException {
		synchronized (this) {
			if (!closed) {
				try {
					// make a best effort attempt to figure out the size
					long size = 0;
					try {
						size = out.getPos();
					} catch (Exception ignored) {}

					// close and return
					out.close();

					final FileStateHandle handle = new FileStateHandle(path, size);
					return new StreamHandleAndPointer(handle, path.toString());
				}
				catch (Exception e) {
					try {
						fileSystem.delete(path, false);
					}
					catch (Exception deleteException) {
						LOG.warn("Could not delete the checkpoint stream file {}.", path, deleteException);
					}

					throw new IOException("Could not flush and close the file system " +
							"output stream to " + path + " in order to obtain the " +
							"stream state handle", e);
				}
				finally {
					closed = true;
				}
			}
			else {
				throw new IOException("Stream has already been closed and discarded.");
			}
		}
	}
}
