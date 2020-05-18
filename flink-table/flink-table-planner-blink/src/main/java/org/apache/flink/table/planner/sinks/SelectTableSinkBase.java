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

package org.apache.flink.table.planner.sinks;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.SelectResultProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;

import java.util.UUID;
import java.util.stream.Stream;

/**
 * Basic implementation of {@link StreamTableSink} for select job to collect the result to local.
 */
public abstract class SelectTableSinkBase<T> implements StreamTableSink<T> {

	private final TableSchema tableSchema;
	protected final DataFormatConverters.DataFormatConverter<RowData, Row> converter;

	private final CollectSinkOperatorFactory<T> factory;
	protected final CollectResultIterator<T> iterator;

	@SuppressWarnings("unchecked")
	public SelectTableSinkBase(TableSchema schema, TypeSerializer<T> typeSerializer) {
		this.tableSchema = SelectTableSinkSchemaConverter.convertTimeAttributeToRegularTimestamp(
			SelectTableSinkSchemaConverter.changeDefaultConversionClass(schema));
		this.converter = DataFormatConverters.getConverterForDataType(this.tableSchema.toPhysicalRowDataType());

		String accumulatorName = "tableResultCollect_" + UUID.randomUUID();
		this.factory = new CollectSinkOperatorFactory<>(typeSerializer, accumulatorName);
		CollectSinkOperator<Row> operator = (CollectSinkOperator<Row>) factory.getOperator();
		this.iterator = new CollectResultIterator<>(operator.getOperatorIdFuture(), typeSerializer, accumulatorName);
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public TableSink<T> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		throw new UnsupportedOperationException();
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<T> dataStream) {
		CollectStreamSink<?> sink = new CollectStreamSink<>(dataStream, factory);
		dataStream.getExecutionEnvironment().addOperator(sink.getTransformation());
		return sink.name("Select table sink");
	}

	public abstract SelectResultProvider getSelectResultProvider();

	/**
	 * Create RowDataTypeInfo based on given table schema.
	 */
	protected static RowDataTypeInfo createRowDataTypeInfo(TableSchema tableSchema) {
		return new RowDataTypeInfo(
				Stream.of(tableSchema.getFieldDataTypes()).map(DataType::getLogicalType).toArray(LogicalType[]::new));
	}
}
