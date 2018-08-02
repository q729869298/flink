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

package org.apache.flink.formats.parquet.utils;

import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class ParquetRecordReaderTest extends TestUtil {

	@Test
	public void testReadSimpleGroup() throws IOException {
		temp.create();
		Configuration configuration = new Configuration();
		GenericData.Record record = new GenericRecordBuilder(SIMPLE_SCHEMA)
			.set("bar", "test")
			.set("foo", 32L).build();

		Path path = createTempParquetFile(temp, SIMPLE_SCHEMA, record,  1);
		MessageType readSchema = (new AvroSchemaConverter()).convert(SIMPLE_SCHEMA);
		ParquetRecordReader<Row> rowReader = new ParquetRecordReader<Row>(new RowReadSupport(), readSchema);

		InputFile inputFile =
			HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path.toUri()), TEST_CONFIGURATION);
		ParquetReadOptions options = ParquetReadOptions.builder().build();
		ParquetFileReader fileReader = new ParquetFileReader(inputFile, options);

		rowReader.initialize(fileReader, TEST_CONFIGURATION);
		assertEquals(true, rowReader.hasNextRecord());

		Row row = rowReader.nextRecord();
		assertEquals(2, row.getArity());
		assertEquals(32L, row.getField(0));
		assertEquals("test", row.getField(1));
		assertEquals(true, rowReader.reachEnd());
	}

	@Test
	public void testReadploNestedGroup() throws IOException {
		temp.create();
		Configuration configuration = new Configuration();
		Schema schema = unWrapSchema(NESTED_SCHEMA.getField("bar").schema());
		GenericData.Record barRecord = new GenericRecordBuilder(schema)
			.set("spam", 31L).build();

		GenericData.Record record = new GenericRecordBuilder(NESTED_SCHEMA)
			.set("foo", 32L)
			.set("bar", barRecord)
			.build();

		Path path = createTempParquetFile(temp, NESTED_SCHEMA, record,  1);
		MessageType readSchema = (new AvroSchemaConverter()).convert(NESTED_SCHEMA);
		ParquetRecordReader<Row> rowReader = new ParquetRecordReader<Row>(new RowReadSupport(), readSchema);

		InputFile inputFile =
			HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path.toUri()), TEST_CONFIGURATION);
		ParquetReadOptions options = ParquetReadOptions.builder().build();
		ParquetFileReader fileReader = new ParquetFileReader(inputFile, options);

		rowReader.initialize(fileReader, TEST_CONFIGURATION);
		assertEquals(true, rowReader.hasNextRecord());

		Row row = rowReader.nextRecord();
		assertEquals(7, row.getArity());
		assertEquals(32L, row.getField(0));
		assertEquals(31L, ((Row) row.getField(2)).getField(0));
		assertEquals(true, rowReader.reachEnd());
	}

	@Test
	public void testMapGroup() throws IOException {
		temp.create();
		Configuration configuration = new Configuration();
		Preconditions.checkState(unWrapSchema(NESTED_SCHEMA.getField("spamMap").schema())
			.getType().equals(Schema.Type.MAP));
		ImmutableMap.Builder<String, String> map = ImmutableMap.<String, String>builder();
		map.put("testKey", "testValue");

		GenericRecord record = new GenericRecordBuilder(NESTED_SCHEMA)
			.set("foo", 32L)
			.set("spamMap", map.build())
			.build();

		Path path = createTempParquetFile(temp, NESTED_SCHEMA, record,  1);
		MessageType readSchema = (new AvroSchemaConverter()).convert(NESTED_SCHEMA);
		ParquetRecordReader<Row> rowReader = new ParquetRecordReader<Row>(new RowReadSupport(), readSchema);

		InputFile inputFile =
			HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path.toUri()), TEST_CONFIGURATION);
		ParquetReadOptions options = ParquetReadOptions.builder().build();
		ParquetFileReader fileReader = new ParquetFileReader(inputFile, options);

		rowReader.initialize(fileReader, TEST_CONFIGURATION);
		assertEquals(true, rowReader.hasNextRecord());

		Row row = rowReader.nextRecord();
		assertEquals(7, row.getArity());

		assertEquals(32L, row.getField(0));
		Map<?, ?> result = (Map<?, ?>) row.getField(1);
		assertEquals(result.get("testKey").toString(), "testValue");
		assertEquals(true, rowReader.reachEnd());
	}

	@Test
	public void testArrayGroup() throws IOException {
		temp.create();
		Schema arraySchema = unWrapSchema(NESTED_SCHEMA.getField("arr").schema());
		Preconditions.checkState(arraySchema.getType().equals(Schema.Type.ARRAY));

		List<Long> arrayData = new ArrayList<>();
		arrayData.add(1L);
		arrayData.add(1000L);

		List<String> arrayString = new ArrayList<>();
		arrayString.add("abcd");

		@SuppressWarnings("unchecked")
		GenericData.Array array = new GenericData.Array(arraySchema, arrayData);

		GenericRecord record = new GenericRecordBuilder(NESTED_SCHEMA)
			.set("foo", 32L)
			.set("arr", array)
			.set("strArray", arrayString)
			.build();

		Path path = createTempParquetFile(temp, NESTED_SCHEMA, record,  1);
		MessageType readSchema = (new AvroSchemaConverter()).convert(NESTED_SCHEMA);
		ParquetRecordReader<Row> rowReader = new ParquetRecordReader<Row>(new RowReadSupport(), readSchema);

		InputFile inputFile =
			HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path.toUri()), TEST_CONFIGURATION);
		ParquetReadOptions options = ParquetReadOptions.builder().build();
		ParquetFileReader fileReader = new ParquetFileReader(inputFile, options);

		rowReader.initialize(fileReader, TEST_CONFIGURATION);
		assertEquals(true, rowReader.hasNextRecord());

		Row row = rowReader.nextRecord();
		assertEquals(7, row.getArity());

		assertEquals(32L, row.getField(0));
		Long[] result = (Long[]) row.getField(3);
		assertEquals(1L, result[0].longValue());
		assertEquals(1000L, result[1].longValue());

		String[] strResult = (String[]) row.getField(4);
		assertEquals("abcd", strResult[0]);
	}

	@Test
	public void testNestedMapGroup() throws IOException {
		temp.create();
		Schema nestedMapSchema = unWrapSchema(NESTED_SCHEMA.getField("nestedMap").schema());
		Preconditions.checkState(nestedMapSchema.getType().equals(Schema.Type.MAP));

		Schema mapValueSchema = nestedMapSchema.getValueType();
		GenericRecord mapValue = new GenericRecordBuilder(mapValueSchema)
			.set("type", "nested")
			.set("value", "nested_value").build();

		ImmutableMap.Builder<String, GenericRecord> map = ImmutableMap.<String, GenericRecord>builder();
		map.put("testKey", mapValue);

		GenericRecord record = new GenericRecordBuilder(NESTED_SCHEMA)
			.set("nestedMap", map.build())
			.set("foo", 34L).build();

		Path path = createTempParquetFile(temp, NESTED_SCHEMA, record,  1);
		MessageType readSchema = (new AvroSchemaConverter()).convert(NESTED_SCHEMA);
		ParquetRecordReader<Row> rowReader = new ParquetRecordReader<Row>(new RowReadSupport(), readSchema);

		InputFile inputFile =
			HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path.toUri()), TEST_CONFIGURATION);
		ParquetReadOptions options = ParquetReadOptions.builder().build();
		ParquetFileReader fileReader = new ParquetFileReader(inputFile, options);

		rowReader.initialize(fileReader, TEST_CONFIGURATION);
		assertEquals(true, rowReader.hasNextRecord());

		Row row = rowReader.nextRecord();
		assertEquals(7, row.getArity());

		assertEquals(34L, row.getField(0));
		Map result = (Map) row.getField(5);

		Row nestedRow = (Row) result.get("testKey");
		assertEquals("nested", nestedRow.getField(0));
		assertEquals("nested_value", nestedRow.getField(1));
	}

	@Test
	public void testNestArrayGroup() throws IOException {
		temp.create();
		Schema nestedArraySchema = unWrapSchema(NESTED_SCHEMA.getField("nestedArray").schema());
		Preconditions.checkState(nestedArraySchema.getType().equals(Schema.Type.ARRAY));

		Schema arrayItemSchema = nestedArraySchema.getElementType();
		GenericRecord item = new GenericRecordBuilder(arrayItemSchema)
			.set("type", "nested")
			.set("value", "nested_value").build();

		ImmutableList.Builder<GenericRecord> list = ImmutableList.<GenericRecord>builder();
		list.add(item);

		GenericRecord record = new GenericRecordBuilder(NESTED_SCHEMA)
			.set("nestedArray", list.build())
			.set("foo", 34L).build();

		Path path = createTempParquetFile(temp, NESTED_SCHEMA, record,  1);
		MessageType readSchema = (new AvroSchemaConverter()).convert(NESTED_SCHEMA);
		ParquetRecordReader<Row> rowReader = new ParquetRecordReader<Row>(new RowReadSupport(), readSchema);

		InputFile inputFile =
			HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path.toUri()), TEST_CONFIGURATION);
		ParquetReadOptions options = ParquetReadOptions.builder().build();
		ParquetFileReader fileReader = new ParquetFileReader(inputFile, options);

		rowReader.initialize(fileReader, TEST_CONFIGURATION);
		assertEquals(true, rowReader.hasNextRecord());

		Row row = rowReader.nextRecord();
		assertEquals(7, row.getArity());

		assertEquals(34L, row.getField(0));
		Object[] result = (Object[]) row.getField(6);

		assertEquals(1, result.length);

		Row nestedRow = (Row) result[0];
		assertEquals("nested", nestedRow.getField(0));
		assertEquals("nested_value", nestedRow.getField(1));
	}
}
