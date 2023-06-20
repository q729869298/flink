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

package org.apache.flink.table.runtime.functions.aggregate;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import java.util.Arrays;
import java.util.List;

/** Built-in LAST_VALUE aggregate function. */
@Internal
public final class LastValueAggFunction<T> extends BuiltInAggregateFunction<T, RowData> {

    private final transient DataType[] valueDataTypes;

    public LastValueAggFunction(LogicalType... valueTypes) {
        this.valueDataTypes =
                Arrays.stream(valueTypes)
                        .map(DataTypeUtils::toInternalDataType)
                        .toArray(DataType[]::new);
    }

    // --------------------------------------------------------------------------------------------
    // Planning
    // --------------------------------------------------------------------------------------------

    @Override
    public List<DataType> getArgumentDataTypes() {
        return Arrays.asList(valueDataTypes);
    }

    @Override
    public DataType getAccumulatorDataType() {
        return DataTypes.ROW(
                        DataTypes.FIELD("lastValue", valueDataTypes[0].nullable()),
                        DataTypes.FIELD("lastOrder", DataTypes.BIGINT()))
                .bridgedTo(RowData.class);
    }

    @Override
    public DataType getOutputDataType() {
        return valueDataTypes[0];
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    @Override
    public RowData createAccumulator() {
        GenericRowData acc = new GenericRowData(2);
        acc.setField(0, null);
        acc.setField(1, Long.MIN_VALUE);
        return acc;
    }

    public void accumulate(RowData rowData, Object value) {
        // todo: what's the default value? default ignore null, following legacy code
        accumulate(rowData, value, true);
    }

    public void accumulate(RowData rowData, Object value, boolean ignoreNull) {
        if (value != null || !ignoreNull) {
            GenericRowData acc = (GenericRowData) rowData;
            acc.setField(0, value);
        }
    }

    public void accumulate(RowData rowData, Object value, Long order) {
        // todo: what's the default value? default ignore null, following legacy code
        accumulate(rowData, value, order, true);
    }

    public void accumulate(RowData rowData, Object value, Long order, boolean ignoreNull) {
        if ((value != null || !ignoreNull) && order != null) {
            GenericRowData acc = (GenericRowData) rowData;
            // todo: how to deal with order = null ? throw exception or just ignore it
            // it's legacy code, such method won't be exposed to user.
            if (acc.getLong(1) < order) {
                acc.setField(0, value);
                acc.setField(1, order);
            }
        }
    }

    public void accumulate(GenericRowData acc, StringData value) {
        // todo: what's the default value? default ignore null, following legacy code
        accumulate(acc, value, true);
    }

    public void accumulate(GenericRowData acc, StringData value, boolean ignoreNull) {
        if (value != null || !ignoreNull) {
            if (value != null) {
                accumulate(acc, (Object) ((BinaryStringData) value).copy());
            } else {
                accumulate(acc, (Object) null);
            }
        }
    }

    public void accumulate(GenericRowData acc, StringData value, Long order) {
        // todo: what's the default value? default ignore null, following legacy code
        accumulate(acc, value, order, true);
    }

    public void accumulate(GenericRowData acc, StringData value, Long order, boolean ignoreNull) {
        if (value != null || !ignoreNull) {
            if (value != null) {
                accumulate(acc, (Object) ((BinaryStringData) value).copy(), order, ignoreNull);
            } else {
                accumulate(acc, (Object) null, order, false);
            }
        }
    }

    public void resetAccumulator(RowData rowData) {
        GenericRowData acc = (GenericRowData) rowData;
        acc.setField(0, null);
        acc.setField(1, Long.MIN_VALUE);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T getValue(RowData rowData) {
        GenericRowData acc = (GenericRowData) rowData;
        return (T) acc.getField(0);
    }
}
