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

public abstract class VersionedIOReadableWritable implements IOReadableWritable, Versioned {

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(getVersion());
	}

	@Override
	public void read(DataInputView in) throws IOException {
		checkFoundVersion(in.readInt());
	}

	protected void checkFoundVersion(int foundVersion) throws IOException {
		if (!isCompatibleVersion(foundVersion)) {
			long expectedVersion = getVersion();
			throw new IOException("Incompatible version: found " + foundVersion + ", required " + expectedVersion);
		}
	}

	public abstract boolean isCompatibleVersion(int version);
}