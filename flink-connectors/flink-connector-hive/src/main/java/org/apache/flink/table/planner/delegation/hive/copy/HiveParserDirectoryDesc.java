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

package org.apache.flink.table.planner.delegation.hive.copy;

/**
 * Counterpart of hive's org.apache.hadoop.hive.ql.plan.CreateTableDesc, but only include the
 * properties needed for "insert into directory".
 */
public class HiveParserDirectoryDesc {
    private final HiveParserBaseSemanticAnalyzer.HiveParserRowFormatParams rowFormatParams;
    private final HiveParserStorageFormat storageFormat;

    public HiveParserDirectoryDesc(
            HiveParserBaseSemanticAnalyzer.HiveParserRowFormatParams rowFormatParams,
            HiveParserStorageFormat storageFormat) {
        this.rowFormatParams = rowFormatParams;
        this.storageFormat = storageFormat;
    }

    public HiveParserBaseSemanticAnalyzer.HiveParserRowFormatParams getRowFormatParams() {
        return rowFormatParams;
    }

    public HiveParserStorageFormat getStorageFormat() {
        return storageFormat;
    }
}
