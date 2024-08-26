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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.binary.BinaryStringDataUtil;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;

import javax.annotation.Nullable;

/** Implementation of {@link BuiltInFunctionDefinitions#BTRIM}. */
@Internal
public class BTrimFunction extends BuiltInScalarFunction {

    public BTrimFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.BTRIM, context);
    }

    public @Nullable StringData eval(@Nullable StringData str) {
        if (str == null) {
            return null;
        }
        return BinaryStringDataUtil.trim((BinaryStringData) str, BinaryStringData.fromString(" "));
    }

    public @Nullable StringData eval(@Nullable StringData str, @Nullable StringData trimStr) {
        if (str == null || trimStr == null) {
            return null;
        }
        return BinaryStringDataUtil.trim((BinaryStringData) str, (BinaryStringData) trimStr);
    }
}
