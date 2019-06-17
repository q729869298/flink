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

package org.apache.flink.table.api;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link TableSchema}.
 */
public class TableSchemaTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testEqualsAndHashCode() {
		// simple test
		TableSchema schema1 = createBasicTableSchemaBuilder().build();
		TableSchema schema2 = createBasicTableSchemaBuilder().build();
		assertEquals(schema1, schema2);
		assertEquals(schema1.hashCode(), schema2.hashCode());

		// with primary key
		TableSchema schema3 = createBasicTableSchemaBuilder()
			.primaryKey("a")
			.build();
		TableSchema schema4 = createBasicTableSchemaBuilder()
			.primaryKey("a")
			.build();
		assertEquals(schema3, schema4);
		assertEquals(schema3.hashCode(), schema4.hashCode());

		// with unique key
		TableSchema schema5 = createBasicTableSchemaBuilder()
			.uniqueKey("a", "b")
			.uniqueKey("b", "c")
			.build();
		TableSchema schema6 = createBasicTableSchemaBuilder()
			.uniqueKey("a", "b")
			.uniqueKey("b", "c")
			.build();
		assertEquals(schema5, schema6);
		assertEquals(schema5.hashCode(), schema6.hashCode());
	}

	@Test
	public void testDuplicatePrimaryKeyAndUniqueKey() {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("The primary key fields have already been declared as unique key");
		createBasicTableSchemaBuilder()
			.uniqueKey("a", "b")
			.primaryKey("a", "b")
			.build();

		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("The unique key have already been declared");
		createBasicTableSchemaBuilder()
			.uniqueKey("a", "b")
			.uniqueKey("a", "b")
			.build();

		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("The unique key fields have already been declared as primary key");
		createBasicTableSchemaBuilder()
			.primaryKey("a", "b")
			.uniqueKey("a", "b")
			.build();
	}

	@Test
	public void testToString() {
		TableSchema.Builder builder = createBasicTableSchemaBuilder();

		String expected = "root\n |-- a: INT\n |-- b: STRING\n |-- c: BIGINT\n";
		assertEquals(expected, builder.build().toString());

		builder.primaryKey("a");
		String expected2 = expected + " |-- PRIMARY KEY (a)\n";
		assertEquals(expected2, builder.build().toString());

		builder
			.uniqueKey("a", "b")
			.uniqueKey("b", "c");
		String expected3 = expected2 + " |-- UNIQUE KEY (a, b)\n |-- UNIQUE KEY (b, c)\n";
		assertEquals(expected3, builder.build().toString());
	}

	@Test
	public void testInvalidPrimaryKeyFieldName() {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("The field 'd' is not existed in the schema");
		createBasicTableSchemaBuilder()
			.primaryKey("a", "d");
	}

	@Test
	public void testInvalidMultiPrimaryKey() {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("A primary key [a] have been defined, can not define another primary key [b]");
		createBasicTableSchemaBuilder()
			.primaryKey("a")
			.primaryKey("b");
	}

	@Test
	public void testInvalidUniqueKeyFieldName() {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("The field 'd' is not existed in the schema");
		createBasicTableSchemaBuilder().uniqueKey("a", "d");
	}

	private TableSchema.Builder createBasicTableSchemaBuilder() {
		return TableSchema.builder()
			.field("a", DataTypes.INT())
			.field("b", DataTypes.STRING())
			.field("c", DataTypes.BIGINT());
	}
}
