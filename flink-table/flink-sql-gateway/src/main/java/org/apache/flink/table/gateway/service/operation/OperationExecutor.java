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

package org.apache.flink.table.gateway.service.operation;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.table.gateway.service.result.ResultFetcher;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.table.operations.BeginStatementSetOperation;
import org.apache.flink.table.operations.EndStatementSetOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.util.CloseableIterator;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** An executor to execute the {@link Operation}. */
public class OperationExecutor {

    private final SessionContext sessionContext;
    private final Configuration executionConfig;

    private static final String JOB_ID = "job id";
    private static final String SET_KEY = "key";
    private static final String SET_VALUE = "value";

    @VisibleForTesting
    public OperationExecutor(SessionContext context, Configuration executionConfig) {
        this.sessionContext = context;
        this.executionConfig = executionConfig;
    }

    public ResultFetcher executeStatement(OperationHandle handle, String statement) {
        // Instantiate the TableEnvironment lazily
        TableEnvironmentInternal tableEnv = sessionContext.createTableEnvironment();
        tableEnv.getConfig().getConfiguration().addAll(executionConfig);

        List<Operation> parsedOperations = tableEnv.getParser().parse(statement);
        if (parsedOperations.size() > 1) {
            throw new UnsupportedOperationException(
                    "Unsupported SQL statement! Execute statement only accepts a single SQL statement or "
                            + "multiple 'INSERT INTO' statements wrapped in a 'STATEMENT SET' block.");
        }
        Operation op = parsedOperations.get(0);
        if (op instanceof SetOperation) {
            return callSetOperation(handle, (SetOperation) op);
        } else if (op instanceof ResetOperation) {
            return callResetOperation(handle, (ResetOperation) op);
        } else if (op instanceof BeginStatementSetOperation) {
            // TODO: support statement set in the FLINK-27837
            throw new UnsupportedOperationException();
        } else if (op instanceof EndStatementSetOperation) {
            // TODO: support statement set in the FLINK-27837
            throw new UnsupportedOperationException();
        } else if (op instanceof ModifyOperation) {
            return callModifyOperations(
                    tableEnv, handle, Collections.singletonList((ModifyOperation) op));
        } else if (op instanceof StatementSetOperation) {
            return callModifyOperations(
                    tableEnv, handle, ((StatementSetOperation) op).getOperations());
        } else {
            TableResultInternal result = tableEnv.executeInternal(op);
            return ResultFetcher.fromTableResult(handle, result);
        }
    }

    private ResultFetcher callSetOperation(OperationHandle handle, SetOperation setOp) {
        if (setOp.getKey().isPresent() && setOp.getValue().isPresent()) {
            sessionContext.setConfig(setOp.getKey().get(), setOp.getValue().get());
            return ResultFetcher.fromTableResult(handle, TableResultInternal.TABLE_RESULT_OK);
        } else if (!setOp.getKey().isPresent() && !setOp.getValue().isPresent()) {
            Map<String, String> configMap = sessionContext.getConfigMap();
            return new ResultFetcher(
                    handle,
                    ResolvedSchema.of(
                            Column.physical(SET_KEY, DataTypes.STRING()),
                            Column.physical(SET_VALUE, DataTypes.STRING())),
                    CloseableIterator.adapterForIterator(
                            configMap.entrySet().stream()
                                    .map(
                                            entry ->
                                                    GenericRowData.of(
                                                            StringData.fromString(entry.getKey()),
                                                            StringData.fromString(
                                                                    entry.getValue())))
                                    .map(row -> (RowData) row)
                                    .iterator()),
                    configMap.size());
        } else {
            // Impossible
            throw new SqlExecutionException("Illegal SetOperation: " + setOp.asSummaryString());
        }
    }

    private ResultFetcher callResetOperation(OperationHandle handle, ResetOperation resetOp) {
        if (resetOp.getKey().isPresent()) {
            sessionContext.resetConfig(resetOp.getKey().get());
        } else {
            sessionContext.resetAllConfigs();
        }
        return ResultFetcher.fromTableResult(handle, TableResultInternal.TABLE_RESULT_OK);
    }

    private ResultFetcher callModifyOperations(
            TableEnvironmentInternal tableEnv,
            OperationHandle handle,
            List<ModifyOperation> modifyOperations) {
        TableResultInternal result = tableEnv.executeInternal(modifyOperations);
        return new ResultFetcher(
                handle,
                ResolvedSchema.of(Column.physical(JOB_ID, DataTypes.STRING())),
                CloseableIterator.adapterForIterator(
                        Collections.singletonList(
                                        (RowData)
                                                GenericRowData.of(
                                                        StringData.fromString(
                                                                result.getJobClient()
                                                                        .orElseThrow(
                                                                                () ->
                                                                                        new SqlExecutionException(
                                                                                                "Can't get job client for the operation."))
                                                                        .getJobID()
                                                                        .toString())))
                                .iterator()),
                1);
    }
}
