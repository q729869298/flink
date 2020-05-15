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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcTestFixture;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.DERBY_EBOOKSHOP_DB;
import static org.apache.flink.table.api.Expressions.$;

/**
 * Test suite for {@link JdbcDynamicLookupFunction}.
 */
@RunWith(Parameterized.class)
public class JdbcTemporayTableITCase extends AbstractTestBase {

	public static final String DB_URL = "jdbc:derby:memory:lookup";
	public static final String LOOKUP_TABLE = "lookup_table";

	private final boolean useCache;

	public JdbcTemporayTableITCase(boolean useCache) {
		this.useCache = useCache;
	}

	@Parameterized.Parameters(name = "Table config = {0}")
	public static Collection<Boolean> parameters() {
		return Arrays.asList(true, false);
	}

	@Before
	public void before() throws ClassNotFoundException, SQLException {
		System.setProperty("derby.stream.error.field", JdbcTestFixture.class.getCanonicalName() + ".DEV_NULL");

		Class.forName(DERBY_EBOOKSHOP_DB.getDriverClass());
		try (
			Connection conn = DriverManager.getConnection(DB_URL + ";create=true");
			Statement stat = conn.createStatement()) {
			stat.executeUpdate("CREATE TABLE " + LOOKUP_TABLE + " (" +
				"id1 INT NOT NULL DEFAULT 0," +
				"id2 INT NOT NULL DEFAULT 0," +
				"comment1 VARCHAR(1000)," +
				"comment2 VARCHAR(1000))");

			Object[][] data = new Object[][]{
				new Object[]{1, 1, "11-c1-v1", "11-c2-v1"},
				new Object[]{1, 1, "11-c1-v2", "11-c2-v2"},
				new Object[]{2, 3, null, "23-c2"},
				new Object[]{2, 5, "25-c1", "25-c2"},
				new Object[]{3, 8, "38-c1", "38-c2"}
			};
			boolean[] surroundedByQuotes = new boolean[]{
				false, false, true, true
			};

			StringBuilder sqlQueryBuilder = new StringBuilder(
				"INSERT INTO " + LOOKUP_TABLE + " (id1, id2, comment1, comment2) VALUES ");
			for (int i = 0; i < data.length; i++) {
				sqlQueryBuilder.append("(");
				for (int j = 0; j < data[i].length; j++) {
					if (data[i][j] == null) {
						sqlQueryBuilder.append("null");
					} else {
						if (surroundedByQuotes[j]) {
							sqlQueryBuilder.append("'");
						}
						sqlQueryBuilder.append(data[i][j]);
						if (surroundedByQuotes[j]) {
							sqlQueryBuilder.append("'");
						}
					}
					if (j < data[i].length - 1) {
						sqlQueryBuilder.append(", ");
					}
				}
				sqlQueryBuilder.append(")");
				if (i < data.length - 1) {
					sqlQueryBuilder.append(", ");
				}
			}
			stat.execute(sqlQueryBuilder.toString());
		}
	}

	@After
	public void clearOutputTable() throws Exception {
		Class.forName(DERBY_EBOOKSHOP_DB.getDriverClass());
		try (
			Connection conn = DriverManager.getConnection(DB_URL);
			Statement stat = conn.createStatement()) {
			stat.execute("DROP TABLE " + LOOKUP_TABLE);
		}
	}

	@Test
	public void test() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);
		StreamITCase.clear();

		Table t = tEnv.fromDataStream(env.fromCollection(Arrays.asList(
			new Tuple2<>(1, 1),
			new Tuple2<>(1, 1),
			new Tuple2<>(2, 3),
			new Tuple2<>(2, 5),
			new Tuple2<>(3, 5),
			new Tuple2<>(3, 8)
		)), $("id1"), $("id2"), $("proctime").proctime());
		tEnv.createTemporaryView("T", t);

		String cacheConfig = ", 'lookup.cache.max-rows'='5000', 'lookup.cache.ttl'='1000000'";
		tEnv.sqlUpdate(
			String.format("create table lookup (" +
			"  id1 INT," +
			"  id2 INT," +
			"  comment1 VARCHAR," +
			"  comment2 VARCHAR" +
			") with(" +
			"  'connector'='jdbc'," +
			"  'url'='" + DB_URL + "'," +
			"  'table'='" + LOOKUP_TABLE + "'" +
			"  %s)", useCache ? cacheConfig : ""));

		String sqlQuery = "SELECT source.id1, source.id2, L.comment1, L.comment2 FROM T AS source " +
			"JOIN lookup for system_time as of source.proctime AS L " +
			"ON source.id1 = L.id1 and source.id2 = L.id2";
		Table result = tEnv.sqlQuery(sqlQuery);

		DataStream<Row> resultSet = tEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,1,11-c1-v1,11-c2-v1");
		expected.add("1,1,11-c1-v1,11-c2-v1");
		expected.add("1,1,11-c1-v2,11-c2-v2");
		expected.add("1,1,11-c1-v2,11-c2-v2");
		expected.add("2,3,null,23-c2");
		expected.add("2,5,25-c1,25-c2");
		expected.add("3,8,38-c1,38-c2");

		StreamITCase.compareWithList(expected);
	}
}
