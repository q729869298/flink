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

package org.apache.flink.table.sources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;

import javax.annotation.Nullable;

import java.util.Map;

/**
  * The {@link DefinedFieldMapping} interface provides a mapping for the fields of the table schema
  * ({@link TableSource#getTableSchema} to fields of the physical returned type
  * {@link TableSource#getReturnType} of a {@link TableSource}.
  *
  * <p>If a {@link TableSource} does not implement the {@link DefinedFieldMapping} interface, the fields of
  * its {@link TableSchema} are mapped to the fields of its return type {@link TypeInformation} by name.
  *
  * <p>If the fields cannot or should not be implicitly mapped by name, an explicit mapping can be
  * provided by implementing this interface.
  * If a mapping is provided, all fields must be explicitly mapped.
  */
public interface DefinedFieldMapping {

	/**
	 * Returns the mapping for the fields of the {@link TableSource}'s {@link TableSchema} to the fields of
	 * its return type {@link TypeInformation}.
	 *
	 * <p>The mapping is done based on field names, e.g., a mapping "name" -> "f1" maps the schema field
	 * "name" to the field "f1" of the return type, for example in this case the second field of a
	 * {@link org.apache.flink.api.java.tuple.Tuple}.
	 *
	 * <p>The returned mapping must map all fields (except proctime and rowtime fields) to the return
	 * type. It can also provide a mapping for fields which are not in the {@link TableSchema} to make
	 * fields in the physical {@link TypeInformation} accessible for a {@code TimestampExtractor}.
	 *
	 * @return A mapping from {@link TableSchema} fields to {@link TypeInformation} fields or
	 * null if no mapping is necessary.
	 */
	@Nullable
	Map<String, String> getFieldMapping();
}
