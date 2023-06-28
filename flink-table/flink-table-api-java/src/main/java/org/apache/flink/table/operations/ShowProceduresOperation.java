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

package org.apache.flink.table.operations;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.functions.SqlLikeUtils;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.flink.table.api.internal.TableResultUtils.buildStringArrayResult;

/**
 * Operation to describe a SHOW PROCEDURES [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT]
 * (LIKE | ILIKE) <sql_like_pattern> ] statement.
 */
public class ShowProceduresOperation implements ExecutableOperation {

    private final @Nullable String catalogName;

    private final @Nullable String databaseName;
    private final @Nullable String preposition;

    protected final boolean useLike;

    private final boolean notLike;

    private final boolean isILike;

    @Nullable private final String sqlLikePattern;

    public ShowProceduresOperation(
            boolean useLike, boolean isNotLike, boolean isILike, String sqlLikePattern) {
        this.catalogName = null;
        this.databaseName = null;
        this.preposition = null;
        this.useLike = useLike;
        this.notLike = isNotLike;
        this.isILike = isILike;
        this.sqlLikePattern = sqlLikePattern;
    }

    public ShowProceduresOperation(
            @Nullable String catalogName,
            @Nullable String databaseName,
            String preposition,
            boolean useLike,
            boolean notLike,
            boolean isILike,
            @Nullable String sqlLikePattern) {
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.preposition = preposition;
        this.useLike = useLike;
        this.isILike = isILike;
        this.notLike = notLike;
        this.sqlLikePattern = sqlLikePattern;
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        final List<String> procedures;
        CatalogManager catalogManager = ctx.getCatalogManager();
        try {
            if (preposition == null) {
                // it's to show current_catalog.current_database
                procedures =
                        catalogManager
                                .getCatalogOrError(catalogManager.getCurrentCatalog())
                                .listProcedures(catalogManager.getCurrentDatabase());
            } else {
                Catalog catalog = catalogManager.getCatalogOrThrowException(catalogName);
                procedures = catalog.listProcedures(databaseName);
            }
        } catch (DatabaseNotExistException e) {
            throw new TableException(
                    String.format(
                            "Fail to show procedures because the Database `%s` to show from/in does not exist in Catalog `%s`.",
                            preposition == null
                                    ? catalogManager.getCurrentDatabase()
                                    : databaseName,
                            preposition == null
                                    ? catalogManager.getCurrentCatalog()
                                    : catalogName));
        }

        final String[] rows;
        if (sqlLikePattern != null) {
            rows =
                    procedures.stream()
                            .filter(
                                    row -> {
                                        boolean likeMatch =
                                                isILike
                                                        ? SqlLikeUtils.iLike(
                                                                row, sqlLikePattern, "\\")
                                                        : SqlLikeUtils.like(
                                                                row, sqlLikePattern, "\\");
                                        return notLike != likeMatch;
                                    })
                            .sorted()
                            .toArray(String[]::new);
        } else {
            rows = procedures.stream().sorted().toArray(String[]::new);
        }
        return buildStringArrayResult("procedure name", rows);
    }

    @Override
    public String asSummaryString() {
        StringBuilder builder = new StringBuilder().append("SHOW PROCEDURES");
        if (this.preposition != null) {
            builder.append(String.format(" %s %s.%s", preposition, catalogName, databaseName));
        }
        if (this.useLike) {
            if (notLike) {
                builder.append(
                        String.format(
                                " %s %s %s", "NOT", isILike ? "ILIKE" : "LIKE", sqlLikePattern));
            } else {
                builder.append(String.format(" %s %s", isILike ? "ILIKE" : "LIKE", sqlLikePattern));
            }
        }
        return builder.toString();
    }
}
