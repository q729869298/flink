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

package org.apache.flink.table.jdbc;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

/** Tests for {@link FlinkResultSetMetaData}. */
public class FlinkResultSetMetaDataTest {
    @Test
    public void testResultSetMetaData() throws Exception {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("v1", DataTypes.BOOLEAN()),
                        Column.physical("v2", DataTypes.TINYINT()),
                        Column.physical("v3", DataTypes.SMALLINT()),
                        Column.physical("v4", DataTypes.INT()),
                        Column.physical("v5", DataTypes.BIGINT()),
                        Column.physical("v6", DataTypes.FLOAT()),
                        Column.physical("v7", DataTypes.DOUBLE()),
                        Column.physical("v8", DataTypes.DECIMAL(10, 5)),
                        Column.physical("v9", DataTypes.STRING()),
                        Column.physical("v10", DataTypes.BYTES()),
                        Column.physical("v11", DataTypes.TIME()),
                        Column.physical("v12", DataTypes.DATE()),
                        Column.physical("v13", DataTypes.TIMESTAMP(6)),
                        Column.physical("v14", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(5)),
                        Column.physical("v15", DataTypes.TIMESTAMP_WITH_TIME_ZONE(4)),
                        Column.physical("v16", DataTypes.ARRAY(DataTypes.INT())),
                        Column.physical("v17", DataTypes.ROW()),
                        Column.physical(
                                "v18", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())));
        FlinkResultSetMetaData metaData = new FlinkResultSetMetaData(schema);

        assertEquals(18, metaData.getColumnCount());
        for (int i = 1; i <= 18; i++) {
            assertEquals("v" + i, metaData.getColumnName(i));
            LogicalType logicalType = schema.getColumnDataTypes().get(i - 1).getLogicalType();
            assertEquals(logicalType.asSummaryString(), metaData.getColumnTypeName(i));
            assertEquals(logicalType.is(LogicalTypeFamily.NUMERIC), metaData.isSigned(i));
        }

        assertEquals(Types.BOOLEAN, metaData.getColumnType(1));
        assertEquals(Boolean.class.getName(), metaData.getColumnClassName(1));
        assertEquals(Types.TINYINT, metaData.getColumnType(2));
        assertEquals(Byte.class.getName(), metaData.getColumnClassName(2));
        assertEquals(Types.SMALLINT, metaData.getColumnType(3));
        assertEquals(Short.class.getName(), metaData.getColumnClassName(3));
        assertEquals(Types.INTEGER, metaData.getColumnType(4));
        assertEquals(Integer.class.getName(), metaData.getColumnClassName(4));
        assertEquals(Types.BIGINT, metaData.getColumnType(5));
        assertEquals(Long.class.getName(), metaData.getColumnClassName(5));
        assertEquals(Types.FLOAT, metaData.getColumnType(6));
        assertEquals(Float.class.getName(), metaData.getColumnClassName(6));
        assertEquals(Types.DOUBLE, metaData.getColumnType(7));
        assertEquals(Double.class.getName(), metaData.getColumnClassName(7));
        assertEquals(Types.DECIMAL, metaData.getColumnType(8));
        assertEquals(BigDecimal.class.getName(), metaData.getColumnClassName(8));
        assertEquals(Types.VARCHAR, metaData.getColumnType(9));
        assertEquals(String.class.getName(), metaData.getColumnClassName(9));
        assertEquals(Types.VARBINARY, metaData.getColumnType(10));
        assertEquals("byte[]", metaData.getColumnClassName(10));
        assertEquals(Types.TIME, metaData.getColumnType(11));
        assertEquals(Time.class.getName(), metaData.getColumnClassName(11));
        assertEquals(Types.DATE, metaData.getColumnType(12));
        assertEquals(Date.class.getName(), metaData.getColumnClassName(12));
        assertEquals(Types.TIMESTAMP, metaData.getColumnType(13));
        assertEquals(Timestamp.class.getName(), metaData.getColumnClassName(13));
        assertEquals(Types.TIMESTAMP, metaData.getColumnType(14));
        assertEquals(Timestamp.class.getName(), metaData.getColumnClassName(14));
        assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, metaData.getColumnType(15));
        assertEquals(OffsetDateTime.class.getName(), metaData.getColumnClassName(15));
        assertEquals(Types.ARRAY, metaData.getColumnType(16));
        assertEquals(Array.class.getName(), metaData.getColumnClassName(16));
        assertEquals(Types.STRUCT, metaData.getColumnType(17));
        assertEquals(RowData.class.getName(), metaData.getColumnClassName(17));
        assertEquals(Types.JAVA_OBJECT, metaData.getColumnType(18));
        assertEquals(Map.class.getName(), metaData.getColumnClassName(18));
    }

    @Test
    public void testInvalidType() {
        ResolvedSchema schema =
                ResolvedSchema.of(Column.physical("v1", DataTypes.MULTISET(DataTypes.STRING())));
        assertThrowsExactly(
                RuntimeException.class,
                () -> new FlinkResultSetMetaData(schema),
                "Not supported type[MULTISET<STRING>]");
    }
}
