/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.io;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * This is the abstract base class for {@link IOReadableWritable} which allows to differentiate between serialization
 * versions.
 */
public abstract class VersionedIOReadableWritable implements IOReadableWritable, Versioned {

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(getVersion());
	}

	@Override
	public void read(DataInputView in) throws IOException {
		checkFoundVersion(in.readInt());
	}

	/**
	 * Checks the compatibility of a version number against the own version.
	 *
	 * @param foundVersion the version found from reading the input stream
	 * @throws VersionMismatchException thrown when serialization versions mismatch
	 */
	protected void checkFoundVersion(int foundVersion) throws VersionMismatchException {
		if (!isCompatibleVersion(foundVersion)) {
			long expectedVersion = getVersion();
			throw new VersionMismatchException(
					"Incompatible version: found " + foundVersion + ", required " + expectedVersion);
		}
	}

	/**
	 * Checks for compatibility between this and the found version.
	 *
	 * @param version version number to compare against.
	 * @return true, iff this is compatible to the passed version.
	 */
	public abstract boolean isCompatibleVersion(int version);
}