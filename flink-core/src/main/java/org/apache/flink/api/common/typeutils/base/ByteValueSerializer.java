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

package org.apache.flink.api.common.typeutils.base;

import java.io.IOException;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.ParameterlessTypeSerializerConfig;
import org.apache.flink.api.common.typeutils.ReconfigureResult;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.ByteValue;

@Internal
public final class ByteValueSerializer extends TypeSerializerSingleton<ByteValue> {

	private static final long serialVersionUID = 1L;
	
	public static final ByteValueSerializer INSTANCE = new ByteValueSerializer();

	public static final ParameterlessTypeSerializerConfig CONFIG =
			new ParameterlessTypeSerializerConfig(ByteValueSerializer.class.getCanonicalName());

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public ByteValue createInstance() {
		return new ByteValue();
	}

	@Override
	public ByteValue copy(ByteValue from) {
		return copy(from, new ByteValue());
	}
	
	@Override
	public ByteValue copy(ByteValue from, ByteValue reuse) {
		reuse.setValue(from.getValue());
		return reuse;
	}

	@Override
	public int getLength() {
		return 1;
	}

	@Override
	public void serialize(ByteValue record, DataOutputView target) throws IOException {
		record.write(target);
	}

	@Override
	public ByteValue deserialize(DataInputView source) throws IOException {
		return deserialize(new ByteValue(), source);
	}
	
	@Override
	public ByteValue deserialize(ByteValue reuse, DataInputView source) throws IOException {
		reuse.read(source);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeByte(source.readByte());
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof ByteValueSerializer;
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting & reconfiguring
	// --------------------------------------------------------------------------------------------

	@Override
	public ParameterlessTypeSerializerConfig snapshotConfiguration() {
		return CONFIG;
	}

	@Override
	public ReconfigureResult reconfigure(TypeSerializerConfigSnapshot configSnapshot) {
		return (configSnapshot.equals(CONFIG) || configSnapshot.equals(ByteSerializer.CONFIG))
			? ReconfigureResult.COMPATIBLE
			: ReconfigureResult.INCOMPATIBLE;
	}
}
