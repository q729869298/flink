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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/** Implementation of {@link BuiltInFunctionDefinitions#MAP_UNION}. */
@Internal
public class MapUnionFunction extends BuiltInScalarFunction {
    private final ArrayData.ElementGetter keyElementGetter;
    private final ArrayData.ElementGetter valueElementGetter;

    private final SpecializedFunction.ExpressionEvaluator keyEqualityEvaluator;
    private transient MethodHandle keyEqualityHandle;

    public MapUnionFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.MAP_UNION, context);
        KeyValueDataType outputType =
                ((KeyValueDataType) context.getCallContext().getOutputDataType().get());
        final DataType keyDataType = outputType.getKeyDataType();
        final DataType valueDataType = outputType.getValueDataType();
        keyElementGetter =
                ArrayData.createElementGetter(outputType.getKeyDataType().getLogicalType());
        valueElementGetter =
                ArrayData.createElementGetter(outputType.getValueDataType().getLogicalType());
        keyEqualityEvaluator =
                context.createEvaluator(
                        $("element1").isEqual($("element2")),
                        DataTypes.BOOLEAN(),
                        DataTypes.FIELD("element1", keyDataType.notNull().toInternal()),
                        DataTypes.FIELD("element2", keyDataType.notNull().toInternal()));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        keyEqualityHandle = keyEqualityEvaluator.open(context);
    }

    public @Nullable MapData eval(@Nullable MapData map1, @Nullable MapData map2) {
        try {
            if (map1 == null || map2 == null) {
                return null;
            }
            if (map1.size() == 0) {
                if (map2.size() == 0) {
                    return map1;
                }
                return map2;
            }
            if (map2.size() == 0) {
                return map1;
            }
            return new MapDataForMapUnion(map1, map2);
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    private class MapDataForMapUnion implements MapData {
        private final GenericArrayData keysArray;
        private final GenericArrayData valuesArray;

        public MapDataForMapUnion(MapData map1, MapData map2) throws Throwable {
            List<Object> keysList = new ArrayList<>();
            List<Object> valuesList = new ArrayList<>();
            boolean isKeyNullExist = false;
            for (int i = 0; i < map2.size(); i++) {
                Object key = keyElementGetter.getElementOrNull(map2.keyArray(), i);
                if (key == null) {
                    isKeyNullExist = true;
                }
                keysList.add(key);
                valuesList.add(valueElementGetter.getElementOrNull(map2.valueArray(), i));
            }

            for (int i = 0; i < map1.size(); i++) {
                final Object key1 = keyElementGetter.getElementOrNull(map1.keyArray(), i);
                final Object value1 = valueElementGetter.getElementOrNull(map1.valueArray(), i);

                boolean keyExists = false;
                if (key1 != null) {
                    for (int j = 0; j < keysList.size(); j++) {
                        final Object key2 = keysList.get(j);
                        if (key2 != null && (boolean) keyEqualityHandle.invoke(key1, key2)) {
                            // If key exists in map2, skip this key-value pair
                            keyExists = true;
                            break;
                        }
                    }
                }
                if (isKeyNullExist && key1 == null) {
                    continue;
                }

                // If key doesn't exist in map2, add the key-value pair from map1
                if (!keyExists) {
                    keysList.add(key1);
                    valuesList.add(value1);
                }
            }
            this.keysArray = new GenericArrayData(keysList.toArray());
            this.valuesArray = new GenericArrayData(valuesList.toArray());
        }

        @Override
        public int size() {
            return keysArray.size();
        }

        @Override
        public ArrayData keyArray() {
            return keysArray;
        }

        @Override
        public ArrayData valueArray() {
            return valuesArray;
        }
    }

    @Override
    public void close() throws Exception {
        keyEqualityEvaluator.close();
    }
}
