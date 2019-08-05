/**
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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;

/**
 * Validator for {@link Parquet}.
 */
@Internal
public class ParquetValidator extends FormatDescriptorValidator {
	public static final String FORMAT_TYPE_VALUE = "parquet";
	public static final String FORMAT_SPECIFIC_CLASS = "format.specific-class";
	public static final String FORMAT_REFLECT_CLASS = "format.reflect-class";
	public static final String FORMAT_PARQUET_SCHEMA = "format.parquet-schema";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		final boolean hasSpecificClass = properties.containsKey(FORMAT_SPECIFIC_CLASS);
		final boolean hasReflectClass = properties.containsKey(FORMAT_REFLECT_CLASS);
		final boolean hasParpuetSchema = properties.containsKey(FORMAT_PARQUET_SCHEMA);

		if ((hasSpecificClass && hasReflectClass) || (hasSpecificClass && hasParpuetSchema) || (hasReflectClass && hasParpuetSchema)) {
			throw new ValidationException("you can only define one of specific class,reflect class,parpuet schema .");
		} else if (hasSpecificClass) {
			properties.validateString(FORMAT_SPECIFIC_CLASS, false, 1);
		} else if (hasReflectClass) {
			properties.validateString(FORMAT_REFLECT_CLASS, false, 1);
		} else if (hasParpuetSchema) {
			properties.validateString(FORMAT_PARQUET_SCHEMA, false, 1);
		} else {
			throw new ValidationException("A definition of an Avro specific record class or parpuet schema or a reflect class is required.");
		}

	}
}
