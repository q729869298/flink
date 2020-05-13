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

package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.SqlPartitionUtils;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.LinkedHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Abstract class to describe statements like ALTER TABLE [[catalogName.] dataBasesName].tableName ...
 */
public abstract class SqlAlterTable extends SqlCall {

	public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("ALTER TABLE", SqlKind.ALTER_TABLE);

	protected final SqlIdentifier tableIdentifier;

	public SqlAlterTable(
			SqlParserPos pos,
			SqlIdentifier tableName) {
		super(pos);
		this.tableIdentifier = requireNonNull(tableName, "tableName should not be null");
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	public SqlIdentifier getTableName() {
		return tableIdentifier;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("ALTER");
		if (isView()) {
			writer.keyword("VIEW");
		} else {
			writer.keyword("TABLE");
		}
		tableIdentifier.unparse(writer, leftPrec, rightPrec);
		SqlNodeList partitionSpec = getPartitionSpec();
		if (partitionSpec != null && partitionSpec.size() > 0) {
			writer.keyword("PARTITION");
			partitionSpec.unparse(writer, getOperator().getLeftPrec(), getOperator().getRightPrec());
		}
	}

	public String[] fullTableName() {
		return tableIdentifier.names.toArray(new String[0]);
	}

	/**
	 * Returns the partition spec if the ALTER should be applied to partitions, and null otherwise.
	 */
	public SqlNodeList getPartitionSpec() {
		return null;
	}

	/**
	 * Get partition spec as key-value strings.
	 */
	public LinkedHashMap<String, String> getPartitionKVs() {
		return SqlPartitionUtils.getPartitionKVs(getPartitionSpec());
	}

	/**
	 * Whether the target object is a view or a table.
	 */
	public boolean isView() {
		return false;
	}
}
