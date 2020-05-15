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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.junit.Assert.assertArrayEquals;

/**
 * Test for the Avro serialization and deserialization schema.
 */
public class AvroRowDataDeSerializationSchemaTest {

	@Test
	public void testSerializeDeserialize() throws Exception {
		final DataType dataType = ROW(
			FIELD("bool", BOOLEAN()),
			FIELD("int", INT()),
			FIELD("bigint", BIGINT()),
			FIELD("float", FLOAT()),
			FIELD("double",DOUBLE()),
			FIELD("name", STRING()),
			FIELD("bytes", BYTES()),
			FIELD("decimal", DECIMAL(9, 6)),
			FIELD("doubles", ARRAY(DOUBLE())),
			FIELD("time", TIME(0)),
			FIELD("date", DATE()),
			FIELD("timestamp3", TIMESTAMP(3)),
			FIELD("timestamp9", TIMESTAMP(9)),
			FIELD("map", MAP(STRING(), BIGINT())),
			FIELD("map2map", MAP(STRING(), MAP(STRING(), INT()))));
		final RowType rowType = (RowType) dataType.getLogicalType();
		final TypeInformation<RowData> typeInfo = new RowDataTypeInfo(rowType);

		final Schema schema = AvroSchemaConverter.convertToSchema(rowType);
		final GenericRecord record = new GenericData.Record(schema);
		record.put(0, true);
		record.put(1, 33);
		record.put(2, 44L);
		record.put(3, 12.34F);
		record.put(4, 23.45);
		record.put(5, "hello avro");
		record.put(6, ByteBuffer.wrap(new byte[]{1, 2, 4, 5, 6, 7, 8, 12}));

		ByteBuffer byteBuffer = ByteBuffer.wrap(DecimalData.fromBigDecimal(
			BigDecimal.valueOf(123456789, 6), 9, 6)
			.toUnscaledBytes());
		record.put(7, byteBuffer);

		List<Double> doubles = new ArrayList<>();
		doubles.add(1.2);
		doubles.add(3.4);
		doubles.add(567.8901);
		record.put(8, doubles);

		record.put(9, 18397);
		record.put(10, 10087);
		record.put(11, 1589530213123L);
		record.put(12, 1589530213123000000L);

		Map<String, Long> map = new HashMap<>();
		map.put("flink", 12L);
		map.put("avro", 23L);
		record.put(13, map);

		Map<String, Map<String, Integer>> map2map = new HashMap<>();
		Map<String, Integer> innerMap = new HashMap<>();
		innerMap.put("inner_key1", 123);
		innerMap.put("inner_key2", 234);
		map2map.put("outer_key", innerMap);
		record.put(14, map2map);

		AvroRowDataSerializationSchema serializationSchema = new AvroRowDataSerializationSchema(rowType);
		serializationSchema.open(null);
		AvroRowDataDeserializationSchema deserializationSchema =
			new AvroRowDataDeserializationSchema(rowType, typeInfo);
		deserializationSchema.open(null);

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		DatumWriter<IndexedRecord> datumWriter = new SpecificDatumWriter<>(schema);
		Encoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
		datumWriter.write(record, encoder);
		encoder.flush();
		byte[] input = byteArrayOutputStream.toByteArray();

		RowData rowData = deserializationSchema.deserialize(input);
		byte[] output = serializationSchema.serialize(rowData);

		assertArrayEquals(input, output);
	}
}
