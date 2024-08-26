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

package org.apache.flink.sql.parser.dql;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Show Functions sql call. The full syntax for show functions is as followings:
 *
 * <pre>{@code
 * SHOW [USER] FUNCTIONS [ ( FROM | IN ) [catalog_name.]database_name ] [ [NOT] (LIKE | ILIKE)
 * <sql_like_pattern> ] statement
 * }</pre>
 */
public class SqlShowFunctions extends SqlShowCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("SHOW FUNCTIONS", SqlKind.OTHER);

    private final boolean requireUser;

    public SqlShowFunctions(
            SqlParserPos pos,
            boolean requireUser,
            String preposition,
            SqlIdentifier databaseName,
            String likeType,
            SqlCharStringLiteral likeLiteral,
            boolean notLike) {
        super(pos, preposition, databaseName, likeType, likeLiteral, notLike);
        this.requireUser = requireUser;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public boolean requireUser() {
        return requireUser;
    }

    @Override
    String getOperationName() {
        return requireUser ? "SHOW USER FUNCTIONS" : "SHOW FUNCTIONS";
    }
}
