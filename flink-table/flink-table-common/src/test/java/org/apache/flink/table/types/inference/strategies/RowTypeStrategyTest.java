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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.inference.TypeStrategiesTestBase;

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

/** Tests for {@link RowTypeStrategy}. */
public class RowTypeStrategyTest extends TypeStrategiesTestBase {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return Arrays.asList(
                TestSpec.forStrategy("Infer a row type", SpecificTypeStrategies.ROW)
                        .inputTypes(DataTypes.BIGINT(), DataTypes.STRING())
                        .expectDataType(
                                DataTypes.ROW(
                                                DataTypes.FIELD("f0", DataTypes.BIGINT()),
                                                DataTypes.FIELD("f1", DataTypes.STRING()))
                                        .notNull()));
    }
}
