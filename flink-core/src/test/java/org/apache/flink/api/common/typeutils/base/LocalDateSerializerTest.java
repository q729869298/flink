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

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

/**
 * A test for the {@link LocalDateSerializer}.
 */
public class LocalDateSerializerTest extends SerializerTestBase<LocalDate> {

	@Override
	protected TypeSerializer<LocalDate> createSerializer() {
		return new LocalDateSerializer();
	}

	@Override
	protected int getLength() {
		return 6;
	}

	@Override
	protected Class<LocalDate> getTypeClass() {
		return LocalDate.class;
	}

	@Override
    protected List<LocalDate> getTestData() {
		return Arrays.asList(
			LocalDate.of(0, 1, 1),
			LocalDate.of(1970, 1, 1),
			LocalDate.of(1990, 10, 14),
			LocalDate.of(2013, 8, 12),
			LocalDate.of(2040, 5, 12)
		);
	}
}
