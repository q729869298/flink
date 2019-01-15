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

package org.apache.flink.api.common.typeutils.base.array;

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;

import org.apache.flink.testutils.migration.MigrationVersion;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Migration tests for primitive array type serializers' snapshots.
 */
@RunWith(Parameterized.class)
public class PrimitiveArraySerializerSnapshotMigrationTest extends TypeSerializerSnapshotMigrationTestBase<Object> {

	public PrimitiveArraySerializerSnapshotMigrationTest(TestSpecification<Object> testSpecification) {
		super(testSpecification);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<Object> testSpecifications() {

		// boolean[]

		final TestSpecification<boolean[]> booleanArray = TestSpecification.<boolean[]>builder(
				"1.6-boolean-primitive-array",
				BooleanPrimitiveArraySerializer.class,
				BooleanPrimitiveArraySerializer.BooleanPrimitiveArraySerializerSnapshot.class,
				MigrationVersion.v1_6)
			.withNewSerializerProvider(() -> BooleanPrimitiveArraySerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-boolean-primitive-array-serializer-snapshot")
			.withTestData("flink-1.6-boolean-primitive-array-serializer-data", 10);

		// byte[]

		final TestSpecification<byte[]> byteArray = TestSpecification.<byte[]>builder(
				"1.6-byte-primitive-array",
				BytePrimitiveArraySerializer.class,
				BytePrimitiveArraySerializer.BytePrimitiveArraySerializerSnapshot.class,
				MigrationVersion.v1_6)
			.withNewSerializerProvider(() -> BytePrimitiveArraySerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-byte-primitive-array-serializer-snapshot")
			.withTestData("flink-1.6-byte-primitive-array-serializer-data", 10);

		// char[]

		final TestSpecification<char[]> charArray = TestSpecification.<char[]>builder(
				"1.6-char-primitive-array",
				CharPrimitiveArraySerializer.class,
				CharPrimitiveArraySerializer.CharPrimitiveArraySerializerSnapshot.class,
				MigrationVersion.v1_6)
			.withNewSerializerProvider(() -> CharPrimitiveArraySerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-char-primitive-array-serializer-snapshot")
			.withTestData("flink-1.6-char-primitive-array-serializer-data", 10);

		// double[]

		final TestSpecification<double[]> doubleArray = TestSpecification.<double[]>builder(
				"1.6-double-primitive-array",
				DoublePrimitiveArraySerializer.class,
				DoublePrimitiveArraySerializer.DoublePrimitiveArraySerializerSnapshot.class,
				MigrationVersion.v1_6)
			.withNewSerializerProvider(() -> DoublePrimitiveArraySerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-double-primitive-array-serializer-snapshot")
			.withTestData("flink-1.6-double-primitive-array-serializer-data", 10);

		// float[]

		final TestSpecification<float[]> floatArray = TestSpecification.<float[]>builder(
				"1.6-float-primitive-array",
				FloatPrimitiveArraySerializer.class,
				FloatPrimitiveArraySerializer.FloatPrimitiveArraySerializerSnapshot.class,
				MigrationVersion.v1_6)
			.withNewSerializerProvider(() -> FloatPrimitiveArraySerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-float-primitive-array-serializer-snapshot")
			.withTestData("flink-1.6-float-primitive-array-serializer-data", 10);

		// int[]

		final TestSpecification<int[]> intArray = TestSpecification.<int[]>builder(
				"1.6-int-primitive-array",
				IntPrimitiveArraySerializer.class,
				IntPrimitiveArraySerializer.IntPrimitiveArraySerializerSnapshot.class,
				MigrationVersion.v1_6)
			.withNewSerializerProvider(() -> IntPrimitiveArraySerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-int-primitive-array-serializer-snapshot")
			.withTestData("flink-1.6-int-primitive-array-serializer-data", 10);

		// long[]

		final TestSpecification<long[]> longArray = TestSpecification.<long[]>builder(
				"1.6-long-primitive-array",
				LongPrimitiveArraySerializer.class,
				LongPrimitiveArraySerializer.LongPrimitiveArraySerializerSnapshot.class,
				MigrationVersion.v1_6)
			.withNewSerializerProvider(() -> LongPrimitiveArraySerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-long-primitive-array-serializer-snapshot")
			.withTestData("flink-1.6-long-primitive-array-serializer-data", 10);

		// short[]

		final TestSpecification<short[]> shortArray = TestSpecification.<short[]>builder(
				"1.6-short-primitive-array",
				ShortPrimitiveArraySerializer.class,
				ShortPrimitiveArraySerializer.ShortPrimitiveArraySerializerSnapshot.class,
				MigrationVersion.v1_6)
			.withNewSerializerProvider(() -> ShortPrimitiveArraySerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-short-primitive-array-serializer-snapshot")
			.withTestData("flink-1.6-short-primitive-array-serializer-data", 10);

		// String[]

		final TestSpecification<String[]> stringArray = TestSpecification.<String[]>builder(
				"1.6-string-array",
				StringArraySerializer.class,
				StringArraySerializer.StringArraySerializerSnapshot.class,
				MigrationVersion.v1_6)
			.withNewSerializerProvider(() -> StringArraySerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-string-array-serializer-snapshot")
			.withTestData("flink-1.6-string-array-serializer-data", 10);

		return Arrays.asList(
			booleanArray,
			byteArray,
			charArray,
			doubleArray,
			floatArray,
			intArray,
			longArray,
			shortArray,
			stringArray
		);
	}

}
