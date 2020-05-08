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

package org.apache.flink.api.java.io.jdbc.dialect;

import org.apache.flink.api.java.io.jdbc.source.row.converter.JDBCRowConverter;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handle the SQL dialect of jdbc driver.
 */
public interface JDBCDialect extends Serializable {

	/**
	 * Check if this dialect instance can handle a certain jdbc url.
	 * @param url the jdbc url.
	 * @return True if the dialect can be applied on the given jdbc url.
	 */
	boolean canHandle(String url);

	/**
	 * Get a row converter for the database according to the given row type.
	 * @param rowType the given row type
	 * @return a row converter for the database
	 */
	JDBCRowConverter getRowConverter(RowType rowType);

	/**
	 * Check if this dialect instance support a specific data type in table schema.
	 * @param schema the table schema.
	 * @exception ValidationException in case of the table schema contains unsupported type.
	 */
	default void validate(TableSchema schema) throws ValidationException {
	}

	/**
	 * @return the default driver class name, if user not configure the driver class name,
	 * then will use this one.
	 */
	default Optional<String> defaultDriverName() {
		return Optional.empty();
	}

	/**
	 * Quotes the identifier. This is used to put quotes around the identifier in case the column
	 * name is a reserved keyword, or in case it contains characters that require quotes (e.g. space).
	 * Default using double quotes {@code "} to quote.
	 */
	default String quoteIdentifier(String identifier) {
		return "\"" + identifier + "\"";
	}

	/**
	 * Get dialect upsert statement, the database has its own upsert syntax, such as Mysql
	 * using DUPLICATE KEY UPDATE, and PostgresSQL using ON CONFLICT... DO UPDATE SET..
	 *
	 * @return None if dialect does not support upsert statement, the writer will degrade to
	 * the use of select + update/insert, this performance is poor.
	 */
	default Optional<String> getUpsertStatement(
			String tableName, String[] fieldNames, String[] uniqueKeyFields) {
		return Optional.empty();
	}

	/**
	 * Get row exists statement by condition fields. Default use SELECT.
	 */
	default String getRowExistsStatement(String tableName, String[] conditionFields) {
		String fieldExpressions = Arrays.stream(conditionFields)
			.map(f -> quoteIdentifier(f) + "=?")
			.collect(Collectors.joining(" AND "));
		return "SELECT 1 FROM " + quoteIdentifier(tableName) + " WHERE " + fieldExpressions;
	}

	/**
	 * Get insert into statement.
	 */
	default String getInsertIntoStatement(String tableName, String[] fieldNames) {
		String columns = Arrays.stream(fieldNames)
			.map(this::quoteIdentifier)
			.collect(Collectors.joining(", "));
		String placeholders = Arrays.stream(fieldNames)
			.map(f -> "?")
			.collect(Collectors.joining(", "));
		return "INSERT INTO " + quoteIdentifier(tableName) +
			"(" + columns + ")" + " VALUES (" + placeholders + ")";
	}

	/**
	 * Get merge into statement (sourceSelect could be slightly different between vendor).
	 */
	default String getMergeIntoStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields, String sourceSelect) {
		final Set<String> uniqueKeyFieldsSet = Arrays.stream(uniqueKeyFields).collect(Collectors.toSet());
		String onClause = Arrays.stream(uniqueKeyFields)
			.map(f -> "t." + quoteIdentifier(f) + "=s." + quoteIdentifier(f))
			.collect(Collectors.joining(", "));
		String updateClause = Arrays.stream(fieldNames)
			.filter(f -> !uniqueKeyFieldsSet.contains(f))
			.map(f -> "t." + quoteIdentifier(f) + "=s." + quoteIdentifier(f))
			.collect(Collectors.joining(", "));
		String insertValueClause = Arrays.stream(fieldNames)
			.map(f -> "s." + quoteIdentifier(f))
			.collect(Collectors.joining(", "));
		String columns = Arrays.stream(fieldNames)
				.map(f -> quoteIdentifier(f))
				.collect(Collectors.joining(", "));
		// if we can't divide schema and table-name is risky to call quoteIdentifier(tableName)
		// for example in SQL-server [tbo].[sometable] is ok but [tbo.sometable] is not
		return "MERGE INTO " + tableName + " t " +
			"USING (" + sourceSelect + ") s " +
			"ON (" + onClause + ")" +
			" WHEN MATCHED THEN UPDATE SET " + updateClause +
			" WHEN NOT MATCHED THEN INSERT (" + columns + ") VALUES (" + insertValueClause + ")";
	}

	/**
	 * Get update one row statement by condition fields, default not use limit 1,
	 * because limit 1 is a sql dialect.
	 */
	default String getUpdateStatement(String tableName, String[] fieldNames, String[] conditionFields) {
		String setClause = Arrays.stream(fieldNames)
			.map(f -> quoteIdentifier(f) + "=?")
			.collect(Collectors.joining(", "));
		String conditionClause = Arrays.stream(conditionFields)
			.map(f -> quoteIdentifier(f) + "=?")
			.collect(Collectors.joining(" AND "));
		return "UPDATE " + quoteIdentifier(tableName) +
			" SET " + setClause +
			" WHERE " + conditionClause;
	}

	/**
	 * Get delete one row statement by condition fields, default not use limit 1,
	 * because limit 1 is a sql dialect.
	 */
	default String getDeleteStatement(String tableName, String[] conditionFields) {
		String conditionClause = Arrays.stream(conditionFields)
			.map(f -> quoteIdentifier(f) + "=?")
			.collect(Collectors.joining(" AND "));
		return "DELETE FROM " + quoteIdentifier(tableName) + " WHERE " + conditionClause;
	}

	/**
	 * Get select fields statement by condition fields. Default use SELECT.
	 */
	default String getSelectFromStatement(String tableName, String[] selectFields, String[] conditionFields) {
		String selectExpressions = Arrays.stream(selectFields)
				.map(this::quoteIdentifier)
				.collect(Collectors.joining(", "));
		String fieldExpressions = Arrays.stream(conditionFields)
				.map(f -> quoteIdentifier(f) + "=?")
				.collect(Collectors.joining(" AND "));
		return "SELECT " + selectExpressions + " FROM " +
				quoteIdentifier(tableName) + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "");
	}
}
