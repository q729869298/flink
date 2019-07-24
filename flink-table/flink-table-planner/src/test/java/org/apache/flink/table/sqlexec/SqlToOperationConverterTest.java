/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.sqlexec;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.internal.BatchTableEnvImpl;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.types.DataType;

import org.apache.calcite.sql.SqlNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Test cases for SqlExecutableStatement. **/
public class SqlToOperationConverterTest {
	private static final ExecutionEnvironment streamExec =
		ExecutionEnvironment.getExecutionEnvironment();
	private static final BatchTableEnvImpl batchEnv =
		(BatchTableEnvImpl) BatchTableEnvironment.create(streamExec);

	private static final FlinkPlannerImpl planner = batchEnv.getFlinkPlanner();

	@Before
	public void before() {
		final String ddl1 = "CREATE TABLE t1 (\n" +
			"  a bigint,\n" +
			"  b varchar, \n" +
			"  c int, \n" +
			"  d varchar\n" +
			")\n" +
			"  PARTITIONED BY (a, d)\n" +
			"  with (\n" +
			"    connector.type = 'filesystem', \n" +
			"    connector.path = 'abc', \n" +
			"    format.type = 'csv', \n" +
			"    `format.fields.0.name` = 'a', \n" +
			"    `format.fields.0.type` = 'BIGINT', \n" +
			"    `format.fields.1.name` = 'b', \n" +
			"    `format.fields.1.type` = 'VARCHAR', \n" +
			"    `format.fields.2.name` = 'c', \n" +
			"    `format.fields.2.type` = 'INT', \n" +
			"    `format.fields.3.name` = 'd', \n" +
			"    `format.fields.3.type` = 'VARCHAR'\n" +
			")\n";
		final String ddl2 = "CREATE TABLE t2 (\n" +
			"  a bigint,\n" +
			"  b varchar, \n" +
			"  c int, \n" +
			"  d varchar\n" +
			")\n" +
			"  PARTITIONED BY (a, d)\n" +
			"  with (\n" +
			"    connector.type = 'filesystem', \n" +
			"    connector.path = 'abc', \n" +
			"    format.type = 'csv', \n" +
			"    `format.fields.0.name` = 'a', \n" +
			"    `format.fields.0.type` = 'BIGINT', \n" +
			"    `format.fields.1.name` = 'b', \n" +
			"    `format.fields.1.type` = 'VARCHAR', \n" +
			"    `format.fields.2.name` = 'c', \n" +
			"    `format.fields.2.type` = 'INT', \n" +
			"    `format.fields.3.name` = 'd', \n" +
			"    `format.fields.3.type` = 'VARCHAR'\n" +
			")\n";
		batchEnv.sqlUpdate(ddl1);
		batchEnv.sqlUpdate(ddl2);
	}

	@After
	public void after() {
		final String ddl1 = "DROP TABLE IF EXISTS t1";
		final String ddl2 = "DROP TABLE IF EXISTS t2";
		batchEnv.sqlUpdate(ddl1);
		batchEnv.sqlUpdate(ddl2);
	}

	@Test
	public void testCreateTable() {
		final String sql = "CREATE TABLE tbl1 (\n" +
			"  a bigint,\n" +
			"  b varchar, \n" +
			"  c int, \n" +
			"  d varchar" +
			")\n" +
			"  PARTITIONED BY (a, d)\n" +
			"  with (\n" +
			"    connector = 'kafka', \n" +
			"    kafka.topic = 'log.test'\n" +
			")\n";
		SqlNode node = planner.parse(sql);
		assert node instanceof SqlCreateTable;
		Operation operation = SqlToOperationConverter.convert(planner, node);
		assert operation instanceof CreateTableOperation;
		CreateTableOperation op = (CreateTableOperation) operation;
		CatalogTable catalogTable = op.getCatalogTable();
		assertEquals(Arrays.asList("a", "d"), catalogTable.getPartitionKeys());
		assertArrayEquals(catalogTable.getSchema().getFieldNames(),
			new String[] {"a", "b", "c", "d"});
		assertArrayEquals(catalogTable.getSchema().getFieldDataTypes(),
			new DataType[]{
				DataTypes.BIGINT(),
				DataTypes.VARCHAR(Integer.MAX_VALUE),
				DataTypes.INT(),
				DataTypes.VARCHAR(Integer.MAX_VALUE)});
	}

	@Test(expected = SqlConversionException.class)
	public void testCreateTableWithPkUniqueKeys() {
		final String sql = "CREATE TABLE tbl1 (\n" +
			"  a bigint,\n" +
			"  b varchar, \n" +
			"  c int, \n" +
			"  d varchar, \n" +
			"  primary key(a), \n" +
			"  unique(a, b) \n" +
			")\n" +
			"  PARTITIONED BY (a, d)\n" +
			"  with (\n" +
			"    connector = 'kafka', \n" +
			"    kafka.topic = 'log.test'\n" +
			")\n";
		SqlNode node = planner.parse(sql);
		assert node instanceof SqlCreateTable;
		SqlToOperationConverter.convert(planner, node);
	}

	@Test
	public void testSqlInsertWithStaticPartition() {
		final String sql = "insert into t1 partition(a=1) select b, c, d from t2";
		FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.HIVE);
		SqlNode node = planner.parse(sql);
		assert node instanceof RichSqlInsert;
		Operation operation = SqlToOperationConverter.convert(planner, node);
		assert operation instanceof CatalogSinkModifyOperation;
		CatalogSinkModifyOperation sinkModifyOperation = (CatalogSinkModifyOperation) operation;
		final Map<String, String> expectedStaticPartitions = new HashMap<>();
		expectedStaticPartitions.put("a", "1");
		assertEquals(expectedStaticPartitions, sinkModifyOperation.getStaticPartitions());
	}

	private FlinkPlannerImpl getPlannerBySqlDialect(SqlDialect sqlDialect) {
		batchEnv.getConfig().setSqlDialect(sqlDialect);
		return batchEnv.getFlinkPlanner();
	}
}
