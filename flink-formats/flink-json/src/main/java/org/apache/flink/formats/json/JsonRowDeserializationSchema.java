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

package org.apache.flink.formats.json;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deserialization schema from JSON to Flink types.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads
 * the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 */
@PublicEvolving
public class JsonRowDeserializationSchema implements DeserializationSchema<Row> {

	private static final long serialVersionUID = -228294330688809195L;

	/** Type information describing the result type. */
	private final RowTypeInfo typeInfo;

	/** Object mapper for parsing the JSON. */
	private final ObjectMapper objectMapper = new ObjectMapper();

	private final RuntimeDeserializationConverterFactory.DeserializationRuntimeConverter runtimeConverter;

	/**
	 * Creates a JSON deserialization schema for the given type information.
	 *
	 * @param typeInfo Type information describing the result type. The field names of {@link Row}
	 *                 are used to parse the JSON properties.
	 * @deprecated use {@link Builder} instead
	 */
	@Deprecated
	public JsonRowDeserializationSchema(TypeInformation<Row> typeInfo) {
		this(typeInfo, new DefaultRuntimeDeserializationConverterFactory(), false);
	}

	/**
	 * Creates a JSON deserialization schema for the given JSON schema.
	 *
	 * @param jsonSchema JSON schema describing the result type
	 *
	 * @see <a href="http://json-schema.org/">http://json-schema.org/</a>
	 * @deprecated use {@link Builder} instead
	 */
	@Deprecated
	public JsonRowDeserializationSchema(String jsonSchema) {
		this(JsonRowSchemaConverter.convert(jsonSchema));
	}

	private JsonRowDeserializationSchema(
			TypeInformation<Row> typeInfo,
			RuntimeDeserializationConverterFactory converterFactory,
			boolean failOnMissingField) {
		checkNotNull(typeInfo, "Type information");
		checkArgument(typeInfo instanceof RowTypeInfo, "Only RowTypeInfo is supported");
		checkNotNull(converterFactory, "Did not provide converter factory.");
		this.typeInfo = (RowTypeInfo) typeInfo;
		this.runtimeConverter = converterFactory.getDeserializationRuntimeConverter(this.typeInfo);
		this.runtimeConverter.setFailOnMissingField(failOnMissingField);
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		try {
			final JsonNode root = objectMapper.readTree(message);
			return (Row) runtimeConverter.convert(objectMapper, root);
		} catch (Throwable t) {
			throw new IOException("Failed to deserialize JSON object.", t);
		}
	}

	@Override
	public boolean isEndOfStream(Row nextElement) {
		return false;
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return typeInfo;
	}

	/**
	 * Builder for {@link JsonRowDeserializationSchema}.
	 */
	@PublicEvolving
	public static class Builder {

		private final RowTypeInfo typeInfo;
		private boolean failOnMissingField = false;
		private RuntimeDeserializationConverterFactory converterFactory;

		/**
		 * Creates a JSON deserialization schema for the given type information.
		 *
		 * @param typeInfo Type information describing the result type. The field names of {@link Row}
		 *                 are used to parse the JSON properties.
		 */
		public Builder(TypeInformation<Row> typeInfo) {
			checkArgument(typeInfo instanceof RowTypeInfo, "Only RowTypeInfo is supported");
			this.typeInfo = (RowTypeInfo) typeInfo;
		}

		/**
		 * Creates a JSON deserialization schema for the given JSON schema.
		 *
		 * @param jsonSchema JSON schema describing the result type
		 *
		 * @see <a href="http://json-schema.org/">http://json-schema.org/</a>
		 */
		public Builder(String jsonSchema) {
			this(JsonRowSchemaConverter.convert(checkNotNull(jsonSchema)));
		}

		/**
		 * Configures schema to fail if a JSON field is missing.
		 *
		 * <p>By default, a missing field is ignored and the field is set to null.
		 */
		public Builder failOnMissingField() {
			this.failOnMissingField = true;
			return this;
		}

		/**
		 * Configures schema to use runtime converter factory.
		 */
		public Builder useConverterFactory(RuntimeDeserializationConverterFactory converterFactory) {
			this.converterFactory = converterFactory;
			return this;
		}

		public JsonRowDeserializationSchema build() {
			RuntimeDeserializationConverterFactory factory =
				converterFactory != null ? converterFactory : new DefaultRuntimeDeserializationConverterFactory();

			return new JsonRowDeserializationSchema(typeInfo, factory, failOnMissingField);
		}
	}

	/**
	 * Configures the failure behaviour if a JSON field is missing.
	 *
	 * <p>By default, a missing field is ignored and the field is set to null.
	 *
	 * @param failOnMissingField Flag indicating whether to fail or not on a missing field.
	 * @deprecated provide the flag during creating the format instead
	 */
	@Deprecated
	public void setFailOnMissingField(boolean failOnMissingField) {
		this.runtimeConverter.setFailOnMissingField(failOnMissingField);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final JsonRowDeserializationSchema that = (JsonRowDeserializationSchema) o;
		return Objects.equals(typeInfo, that.typeInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(typeInfo);
	}
}
