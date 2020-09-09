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

package org.apache.flink.formats.json.debezium;

import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link DebeziumJsonSerializationSchema} and {@link DebeziumJsonDeserializationSchema}.
 */
public class DebeziumJsonSerDeSchemaTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static final RowType SCHEMA = (RowType) ROW(
		FIELD("id", INT().notNull()),
		FIELD("name", STRING()),
		FIELD("description", STRING()),
		FIELD("weight", FLOAT())
	).getLogicalType();

	@Test
	public void testSerializationAndSchemaIncludeDeserialization() throws Exception {
		testSerializationDeserialization("debezium-data-schema-include.txt", true);
	}

	@Test
	public void testSerializationAndSchemaExcludeDeserialization() throws Exception {
		testSerializationDeserialization("debezium-data-schema-exclude.txt", false);
	}

	@Test
	public void testSerializationAndPostgresSchemaIncludeDeserialization() throws Exception {
		testSerializationDeserialization("debezium-postgres-data-schema-include.txt", true);
	}

	@Test
	public void testSerializationAndPostgresSchemaExcludeDeserialization() throws Exception {
		testSerializationDeserialization("debezium-postgres-data-schema-exclude.txt", false);
	}

	@Test
	public void testPostgresDefaultReplicaIdentify() throws Exception {
		try {
			testSerializationDeserialization("debezium-postgres-data-replica-identity.txt", false);
		} catch (Exception e) {
			assertTrue(ExceptionUtils.findThrowableWithMessage(e,
				"The \"before\" field of UPDATE message is null, if you are using Debezium Postgres Connector, " +
					"please check the Postgres table has been set REPLICA IDENTITY to FULL level.").isPresent());
		}
	}

	@Test
	public void testTombstoneMessages() throws Exception {
		DebeziumJsonDeserializationSchema deserializationSchema = new DebeziumJsonDeserializationSchema(
			SCHEMA,
			InternalTypeInfo.of(SCHEMA),
			false,
			false,
			TimestampFormat.ISO_8601);
		SimpleCollector collector = new SimpleCollector();
		deserializationSchema.deserialize(null, collector);
		deserializationSchema.deserialize(new byte[]{}, collector);
		assertTrue(collector.list.isEmpty());
	}

	private void testSerializationDeserialization(String resourceFile, boolean schemaInclude) throws Exception {
		List<String> lines = readLines(resourceFile);
		DebeziumJsonDeserializationSchema deserializationSchema = new DebeziumJsonDeserializationSchema(
			SCHEMA,
			InternalTypeInfo.of(SCHEMA),
			schemaInclude,
			false,
			TimestampFormat.ISO_8601);

		SimpleCollector collector = new SimpleCollector();
		for (String line : lines) {
			deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8), collector);
		}

		// Debezium captures change data (`debezium-data-schema-include.txt`) on the `product` table:
		//
		// CREATE TABLE product (
		//  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
		//  name VARCHAR(255),
		//  description VARCHAR(512),
		//  weight FLOAT
		// );
		// ALTER TABLE product AUTO_INCREMENT = 101;
		//
		// INSERT INTO product
		// VALUES (default,"scooter","Small 2-wheel scooter",3.14),
		//        (default,"car battery","12V car battery",8.1),
		//        (default,"12-pack drill bits","12-pack of drill bits with sizes ranging from #40 to #3",0.8),
		//        (default,"hammer","12oz carpenter's hammer",0.75),
		//        (default,"hammer","14oz carpenter's hammer",0.875),
		//        (default,"hammer","16oz carpenter's hammer",1.0),
		//        (default,"rocks","box of assorted rocks",5.3),
		//        (default,"jacket","water resistent black wind breaker",0.1),
		//        (default,"spare tire","24 inch spare tire",22.2);
		// UPDATE product SET description='18oz carpenter hammer' WHERE id=106;
		// UPDATE product SET weight='5.1' WHERE id=107;
		// INSERT INTO product VALUES (default,"jacket","water resistent white wind breaker",0.2);
		// INSERT INTO product VALUES (default,"scooter","Big 2-wheel scooter ",5.18);
		// UPDATE product SET description='new water resistent white wind breaker', weight='0.5' WHERE id=110;
		// UPDATE product SET weight='5.17' WHERE id=111;
		// DELETE FROM product WHERE id=111;
		List<String> expected = Arrays.asList(
			"+I(101,scooter,Small 2-wheel scooter,3.14)",
			"+I(102,car battery,12V car battery,8.1)",
			"+I(103,12-pack drill bits,12-pack of drill bits with sizes ranging from #40 to #3,0.8)",
			"+I(104,hammer,12oz carpenter's hammer,0.75)",
			"+I(105,hammer,14oz carpenter's hammer,0.875)",
			"+I(106,hammer,16oz carpenter's hammer,1.0)",
			"+I(107,rocks,box of assorted rocks,5.3)",
			"+I(108,jacket,water resistent black wind breaker,0.1)",
			"+I(109,spare tire,24 inch spare tire,22.2)",
			"-U(106,hammer,16oz carpenter's hammer,1.0)",
			"+U(106,hammer,18oz carpenter hammer,1.0)",
			"-U(107,rocks,box of assorted rocks,5.3)",
			"+U(107,rocks,box of assorted rocks,5.1)",
			"+I(110,jacket,water resistent white wind breaker,0.2)",
			"+I(111,scooter,Big 2-wheel scooter ,5.18)",
			"-U(110,jacket,water resistent white wind breaker,0.2)",
			"+U(110,jacket,new water resistent white wind breaker,0.5)",
			"-U(111,scooter,Big 2-wheel scooter ,5.18)",
			"+U(111,scooter,Big 2-wheel scooter ,5.17)",
			"-D(111,scooter,Big 2-wheel scooter ,5.17)"
		);
		List<String> actual = collector.list.stream()
			.map(Object::toString)
			.collect(Collectors.toList());
		assertEquals(expected, actual);

		DebeziumJsonSerializationSchema serializationSchema = new DebeziumJsonSerializationSchema(
			SCHEMA,
			TimestampFormat.SQL);
		serializationSchema.open(null);
		actual = new ArrayList<>();
		for (RowData rowData : collector.list) {
			actual.add(new String(serializationSchema.serialize(rowData), StandardCharsets.UTF_8));
		}

		expected = Arrays.asList(
			"{\"before\":null,\"after\":{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":3.14},\"op\":\"c\"}",
			"{\"before\":null,\"after\":{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":8.1},\"op\":\"c\"}",
			"{\"before\":null,\"after\":{\"id\":103,\"name\":\"12-pack drill bits\",\"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\"weight\":0.8},\"op\":\"c\"}",
			"{\"before\":null,\"after\":{\"id\":104,\"name\":\"hammer\",\"description\":\"12oz carpenter's hammer\",\"weight\":0.75},\"op\":\"c\"}",
			"{\"before\":null,\"after\":{\"id\":105,\"name\":\"hammer\",\"description\":\"14oz carpenter's hammer\",\"weight\":0.875},\"op\":\"c\"}",
			"{\"before\":null,\"after\":{\"id\":106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\",\"weight\":1.0},\"op\":\"c\"}",
			"{\"before\":null,\"after\":{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":5.3},\"op\":\"c\"}",
			"{\"before\":null,\"after\":{\"id\":108,\"name\":\"jacket\",\"description\":\"water resistent black wind breaker\",\"weight\":0.1},\"op\":\"c\"}",
			"{\"before\":null,\"after\":{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":22.2},\"op\":\"c\"}",
			"{\"before\":null,\"after\":{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":22.2},\"op\":\"d\"}",
			"{\"before\":null,\"after\":{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":22.2},\"op\":\"c\"}",
			"{\"before\":null,\"after\":{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":22.2},\"op\":\"d\"}",
			"{\"before\":null,\"after\":{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":22.2},\"op\":\"c\"}",
			"{\"before\":null,\"after\":{\"id\":110,\"name\":\"jacket\",\"description\":\"water resistent white wind breaker\",\"weight\":0.2},\"op\":\"c\"}",
			"{\"before\":null,\"after\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.18},\"op\":\"c\"}",
			"{\"before\":null,\"after\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.18},\"op\":\"d\"}",
			"{\"before\":null,\"after\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.18},\"op\":\"c\"}",
			"{\"before\":null,\"after\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.18},\"op\":\"d\"}",
			"{\"before\":null,\"after\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.18},\"op\":\"c\"}",
			"{\"before\":{\"id\":111,\"name\":\"scooter\",\"description\":\"Big 2-wheel scooter \",\"weight\":5.17},\"after\":null,\"op\":\"d\"}"
		);
		assertEquals(expected, actual);
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	private static List<String> readLines(String resource) throws IOException {
		final URL url = DebeziumJsonSerDeSchemaTest.class.getClassLoader().getResource(resource);
		assert url != null;
		Path path = new File(url.getFile()).toPath();
		return Files.readAllLines(path);
	}

	private static class SimpleCollector implements Collector<RowData> {

		private List<RowData> list = new ArrayList<>();

		@Override
		public void collect(RowData record) {
			list.add(record);
		}

		@Override
		public void close() {
			// do nothing
		}
	}
}
