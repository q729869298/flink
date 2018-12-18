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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat.JDBCGauge;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat.Ticker;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link JDBCOutputFormat}.
 */
public class JDBCOutputFormatTest extends JDBCTestBase {

	private JDBCOutputFormat jdbcOutputFormat;

	@After
	public void tearDown() throws IOException {
		if (jdbcOutputFormat != null) {
			jdbcOutputFormat.close();
		}
		jdbcOutputFormat = null;
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidDriver() throws IOException {
		jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername("org.apache.derby.jdbc.idontexist")
				.setDBUrl(DB_URL)
				.setQuery(String.format(INSERT_TEMPLATE, INPUT_TABLE))
				.finish();
		jdbcOutputFormat.open(0, 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidURL() throws IOException {
		jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl("jdbc:der:iamanerror:mory:ebookshop")
				.setQuery(String.format(INSERT_TEMPLATE, INPUT_TABLE))
				.finish();
		jdbcOutputFormat.open(0, 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidQuery() throws IOException {
		jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery("iamnotsql")
				.finish();
		jdbcOutputFormat.open(0, 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testIncompleteConfiguration() throws IOException {
		jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername(DRIVER_CLASS)
				.setQuery(String.format(INSERT_TEMPLATE, INPUT_TABLE))
				.finish();
	}

	@Test(expected = RuntimeException.class)
	public void testIncompatibleTypes() throws IOException {
		jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(String.format(INSERT_TEMPLATE, INPUT_TABLE))
				.finish();
		jdbcOutputFormat.setRuntimeContext(createMockRuntimeContext());
		jdbcOutputFormat.open(0, 1);

		Row row = new Row(5);
		row.setField(0, 4);
		row.setField(1, "hello");
		row.setField(2, "world");
		row.setField(3, 0.99);
		row.setField(4, "imthewrongtype");

		jdbcOutputFormat.writeRecord(row);
		jdbcOutputFormat.close();
	}

	@Test(expected = RuntimeException.class)
	public void testExceptionOnInvalidType() throws IOException {
		jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
			.setDrivername(DRIVER_CLASS)
			.setDBUrl(DB_URL)
			.setQuery(String.format(INSERT_TEMPLATE, OUTPUT_TABLE))
			.setSqlTypes(new int[] {
				Types.INTEGER,
				Types.VARCHAR,
				Types.VARCHAR,
				Types.DOUBLE,
				Types.INTEGER})
			.finish();
		jdbcOutputFormat.setRuntimeContext(createMockRuntimeContext());
		jdbcOutputFormat.open(0, 1);

		JDBCTestBase.TestEntry entry = TEST_DATA[0];
		Row row = new Row(5);
		row.setField(0, entry.id);
		row.setField(1, entry.title);
		row.setField(2, entry.author);
		row.setField(3, 0L); // use incompatible type (Long instead of Double)
		row.setField(4, entry.qty);
		jdbcOutputFormat.writeRecord(row);
	}

	@Test(expected = RuntimeException.class)
	public void testExceptionOnClose() throws IOException {

		jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
			.setDrivername(DRIVER_CLASS)
			.setDBUrl(DB_URL)
			.setQuery(String.format(INSERT_TEMPLATE, OUTPUT_TABLE))
			.setSqlTypes(new int[] {
				Types.INTEGER,
				Types.VARCHAR,
				Types.VARCHAR,
				Types.DOUBLE,
				Types.INTEGER})
			.finish();
		jdbcOutputFormat.setRuntimeContext(createMockRuntimeContext());
		jdbcOutputFormat.open(0, 1);

		JDBCTestBase.TestEntry entry = TEST_DATA[0];
		Row row = new Row(5);
		row.setField(0, entry.id);
		row.setField(1, entry.title);
		row.setField(2, entry.author);
		row.setField(3, entry.price);
		row.setField(4, entry.qty);
		jdbcOutputFormat.writeRecord(row);
		jdbcOutputFormat.writeRecord(row); // writing the same record twice must yield a unique key violation.

		jdbcOutputFormat.close();
	}

	@Test
	public void testJDBCOutputFormat() throws IOException, SQLException {
		jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername(DRIVER_CLASS)
				.setDBUrl(DB_URL)
				.setQuery(String.format(INSERT_TEMPLATE, OUTPUT_TABLE))
				.finish();

		jdbcOutputFormat.setRuntimeContext(createMockRuntimeContext());
		jdbcOutputFormat.open(0, 1);

		for (JDBCTestBase.TestEntry entry : TEST_DATA) {
			jdbcOutputFormat.writeRecord(toRow(entry));
		}

		jdbcOutputFormat.close();

		try (
			Connection dbConn = DriverManager.getConnection(DB_URL);
			PreparedStatement statement = dbConn.prepareStatement(JDBCTestBase.SELECT_ALL_NEWBOOKS);
			ResultSet resultSet = statement.executeQuery()
		) {
			int recordCount = 0;
			while (resultSet.next()) {
				assertEquals(TEST_DATA[recordCount].id, resultSet.getObject("id"));
				assertEquals(TEST_DATA[recordCount].title, resultSet.getObject("title"));
				assertEquals(TEST_DATA[recordCount].author, resultSet.getObject("author"));
				assertEquals(TEST_DATA[recordCount].price, resultSet.getObject("price"));
				assertEquals(TEST_DATA[recordCount].qty, resultSet.getObject("qty"));

				recordCount++;
			}
			assertEquals(TEST_DATA.length, recordCount);
		}
	}

	@Test
	public void testFlush() throws SQLException, IOException {
		jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
			.setDrivername(DRIVER_CLASS)
			.setDBUrl(DB_URL)
			.setQuery(String.format(INSERT_TEMPLATE, OUTPUT_TABLE_2))
			.setBatchInterval(3)
			.finish();
		try (
			Connection dbConn = DriverManager.getConnection(DB_URL);
			PreparedStatement statement = dbConn.prepareStatement(JDBCTestBase.SELECT_ALL_NEWBOOKS_2)
		) {
			jdbcOutputFormat.setRuntimeContext(createMockRuntimeContext());
			jdbcOutputFormat.open(0, 1);
			for (int i = 0; i < 2; ++i) {
				jdbcOutputFormat.writeRecord(toRow(TEST_DATA[i]));
			}
			try (ResultSet resultSet = statement.executeQuery()) {
				assertFalse(resultSet.next());
			}
			jdbcOutputFormat.writeRecord(toRow(TEST_DATA[2]));
			try (ResultSet resultSet = statement.executeQuery()) {
				int recordCount = 0;
				while (resultSet.next()) {
					assertEquals(TEST_DATA[recordCount].id, resultSet.getObject("id"));
					assertEquals(TEST_DATA[recordCount].title, resultSet.getObject("title"));
					assertEquals(TEST_DATA[recordCount].author, resultSet.getObject("author"));
					assertEquals(TEST_DATA[recordCount].price, resultSet.getObject("price"));
					assertEquals(TEST_DATA[recordCount].qty, resultSet.getObject("qty"));
					recordCount++;
				}
				assertEquals(3, recordCount);
			}
		} finally {
			jdbcOutputFormat.close();
		}
	}

	@Test
	public void testMetrics() throws IOException {
		Ticker tickerMock = mock(Ticker.class);
		when(tickerMock.read()).thenReturn(0L);
		jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
			.setDrivername(DRIVER_CLASS)
			.setDBUrl(DB_URL)
			.setQuery(String.format(INSERT_TEMPLATE, OUTPUT_TABLE_2))
			.setBatchInterval(3)
			.setTicker(tickerMock)
			.finish();
		Tuple4<RuntimeContext, MetricGroup, MetricGroup, Meter> mocks = createMocks();
		RuntimeContext ctxMock = mocks.f0;
		MetricGroup mgrMock1 = mocks.f1;
		MetricGroup mgrMock2 = mocks.f2;
		Meter flushRateMeterMock = mocks.f3;
		try {
			jdbcOutputFormat.setRuntimeContext(ctxMock);
			jdbcOutputFormat.open(0, 1);
			for (int i = 3; i < 6; ++i) {
				jdbcOutputFormat.writeRecord(toRow(TEST_DATA[i]));
			}
		} finally {
			jdbcOutputFormat.close();
		}
		verify(ctxMock, times(3)).getMetricGroup();
		verify(mgrMock1, times(3)).addGroup(JDBCOutputFormat.FLUSH_SCOPE);
		verify(mgrMock2).meter(eq(JDBCOutputFormat.FLUSH_RATE_NAME), any(MeterView.class));
		verify(flushRateMeterMock, times(2)).markEvent(); // One for actual flush; Second for close().
		verify(tickerMock, times(4)).read(); // Two for actual flush; Another two for close().
		verify(mgrMock2).gauge(eq(JDBCOutputFormat.FLUSH_DURATION_NAME), any(JDBCGauge.class));
		verify(mgrMock2).gauge(eq(JDBCOutputFormat.FLUSH_BATCH_SIZE_NAME), any(JDBCGauge.class));
		verifyZeroInteractions(ctxMock, mgrMock1, mgrMock2, tickerMock);
	}

	@After
	public void clearOutputTable() throws Exception {
		Class.forName(DRIVER_CLASS);
		try (
			Connection conn = DriverManager.getConnection(DB_URL);
			Statement stat = conn.createStatement()) {
			stat.execute("DELETE FROM " + OUTPUT_TABLE);

			stat.close();
			conn.close();
		}
	}

	private static Row toRow(TestEntry entry) {
		Row row = new Row(5);
		row.setField(0, entry.id);
		row.setField(1, entry.title);
		row.setField(2, entry.author);
		row.setField(3, entry.price);
		row.setField(4, entry.qty);
		return row;
	}

	private Tuple4<RuntimeContext, MetricGroup, MetricGroup, Meter> createMocks() {
		RuntimeContext ctxMock = mock(RuntimeContext.class);
		MetricGroup mgrMock1 = mock(MetricGroup.class);
		MetricGroup mgrMock2 = mock(MetricGroup.class);
		MeterView flushRateMeterMock = mock(MeterView.class);
		when(ctxMock.getMetricGroup()).thenReturn(mgrMock1);
		when(mgrMock1.addGroup(eq(JDBCOutputFormat.FLUSH_SCOPE))).thenReturn(mgrMock2);
		when(mgrMock2.meter(eq(JDBCOutputFormat.FLUSH_RATE_NAME), any(MeterView.class)))
			.thenReturn(flushRateMeterMock);
		return Tuple4.of(ctxMock, mgrMock1, mgrMock2, flushRateMeterMock);
	}

	private RuntimeContext createMockRuntimeContext() {
		return createMocks().f0;
	}

}
