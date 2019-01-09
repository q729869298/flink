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

package org.apache.flink.modelserving.java.server.typeschema;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Byte Array Schema - used for Kafka byte array serialization/deserialization.
 */
public class ByteArraySchema implements DeserializationSchema<byte[]>, SerializationSchema<byte[]> {

	private long serialVersionUID = 1234567L;

	/**
	 * Deserialize byte array message.
	 * @param message Byte array message.
	 * @return deserialized message.
	 */
	@Override
	public byte[] deserialize(byte[] message) throws IOException {
		return message;
	}

	/**
	 * Check whether end of file is reached.
	 * @param nextElement pointer to the next element.
	 * @return boolean specifying wheather end of stream is reached.
	 */
	@Override
	public boolean isEndOfStream(byte[] nextElement) {
		return false;
	}

	/**
	 * Serialize byte array message.
	 * @param element Byte array for next element.
	 * @return serialized message.
	 */
	@Override
	public byte[] serialize(byte[] element) {
		return element;
	}

	/**
	 * Get data type.
	 * @return data type.
	 */
	@Override
	public TypeInformation<byte[]> getProducedType() {
		return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
	}
}
