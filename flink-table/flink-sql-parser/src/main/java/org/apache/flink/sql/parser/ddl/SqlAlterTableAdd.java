/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/**
 * SqlNode to describe ALTER TABLE table_name ADD column/constraint/watermark clause.
 *
 * <p>Example: DDL like the below for add column/constraint/watermark.
 *
 * <pre>{@code
 * -- add single column
 * ALTER TABLE mytable ADD new_column STRING COMMENT 'new_column docs';
 *
 * -- add multiple columns, constraint, and watermark
 * ALTER TABLE mytable ADD (
 *     log_ts STRING COMMENT 'log timestamp string' FIRST,
 *     ts AS TO_TIMESTAMP(log_ts) AFTER log_ts,
 *     col_meta int metadata from 'mk1' virtual AFTER col_b,
 *     PRIMARY KEY (id) NOT ENFORCED,
 *     WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
 * );
 * }</pre>
 */
public class SqlAlterTableAdd extends SqlAlterTable {

    private final SqlNodeList addedColumns;
    @Nullable private final SqlWatermark watermark;
    private final List<SqlTableConstraint> constraint;

    public SqlAlterTableAdd(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList addedColumns,
            @Nullable SqlWatermark sqlWatermark,
            List<SqlTableConstraint> constraint) {
        super(pos, tableName, null);
        this.addedColumns = addedColumns;
        this.watermark = sqlWatermark;
        this.constraint = constraint;
    }

    public SqlNodeList getColumns() {
        return addedColumns;
    }

    public Optional<SqlWatermark> getWatermark() {
        return Optional.ofNullable(watermark);
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                getTableName(),
                addedColumns,
                watermark,
                new SqlNodeList(constraint, SqlParserPos.ZERO));
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
        writer.keyword("ADD");

        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
        for (SqlNode column : addedColumns.getList()) {
            printIndent(writer);
            column.unparse(writer, leftPrec, rightPrec);
        }
        for (SqlTableConstraint constraint : constraint) {
            printIndent(writer);
            constraint.unparse(writer, leftPrec, rightPrec);
        }
        if (watermark != null) {
            printIndent(writer);
            watermark.unparse(writer, leftPrec, rightPrec);
        }

        writer.newlineAndIndent();
        writer.endList(frame);
    }
}
