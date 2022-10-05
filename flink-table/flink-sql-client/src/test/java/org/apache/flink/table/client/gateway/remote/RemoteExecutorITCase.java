/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.client.gateway.remote;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.config.ResultMode;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.remote.result.TableResultWrapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointExtension;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.table.utils.UserDefinedFunctions;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.util.StringUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_MAX_TABLE_RESULT_ROWS;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_RESULT_MODE;
import static org.assertj.core.api.Assertions.assertThat;

/** Basic tests for the {@link RemoteExecutor}. */
@SuppressWarnings("BusyWait")
public class RemoteExecutorITCase {
    @RegisterExtension
    @Order(1)
    public static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension();

    @RegisterExtension
    @Order(2)
    public static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(MINI_CLUSTER::getClientConfiguration);

    @RegisterExtension
    @Order(3)
    private static final SqlGatewayRestEndpointExtension SQL_GATEWAY_REST_ENDPOINT_EXTENSION =
            new SqlGatewayRestEndpointExtension(SQL_GATEWAY_SERVICE_EXTENSION::getService);

    private static RemoteExecutor executor;
    private final long testMethodTimeout = 100_000L;
    private static final long executionTimeout = 90_000L;
    private static SessionHandle sessionHandle;

    // todo : replace fake results with real results
    // --------------------------------------------------------------------------------------------
    // Expected results for tests
    // --------------------------------------------------------------------------------------------

    private final List<String> expectedResults1 =
            Arrays.asList(
                    "[47, Hello World, ABC]",
                    "[27, Hello World, ABC]",
                    "[37, Hello World, ABC]",
                    "[37, Hello World, ABC]",
                    "[47, Hello World, ABC]",
                    "[57, Hello World!!!!, ABC]");

    private final List<String> expectedResults2 =
            Arrays.asList(
                    "[47, Hello World]",
                    "[27, Hello World]",
                    "[37, Hello World]",
                    "[37, Hello World]",
                    "[47, Hello World]",
                    "[57, Hello World!!!!]");

    private final List<String> expectedResults3 = Collections.singletonList("[1, Hello World!!!!]");

    private final List<String> expectedResults4 =
            Arrays.asList(
                    "[47, ABC]", "[27, ABC]", "[37, ABC]", "[37, ABC]", "[47, ABC]", "[57, ABC]");

    private final List<String> expectedResults5 =
            Arrays.asList("[47]", "[27]", "[37]", "[37]", "[47]", "[57]");

    @BeforeAll
    public static void setUp() {
        executor =
                new RemoteExecutor(
                        MINI_CLUSTER.getClientConfiguration(),
                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress(),
                        SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort());
        executor.start();
        sessionHandle = executor.getSessionHandle();

        URL url = RemoteExecutorITCase.class.getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
        initSession(replaceVars);
    }

    @AfterAll
    public static void cleanup() {
        executor.close();
    }

    // --------------------------------------------------------------------------------------------
    // Runtime mode: streaming; Result mode: changelog
    // --------------------------------------------------------------------------------------------

    @Test
    @Timeout(value = testMethodTimeout, unit = TimeUnit.MILLISECONDS)
    public void testStreamQueryExecutionChangelog() throws Exception {
        executeQuery(
                "SELECT scalarUDF(IntegerField1, 5), StringField1, 'ABC' FROM TableNumber1;",
                buildDefaultExecutionConfiguration(),
                false,
                this::retrieveChangelogResult,
                fakeResult(6));
    }

    @Test
    @Timeout(value = 3 * testMethodTimeout, unit = TimeUnit.MILLISECONDS)
    public void testStreamQueryExecutionChangelogMultipleTimes() throws Exception {
        for (int i = 0; i < 3; i++) {
            executeQuery(
                    "SELECT scalarUDF(IntegerField1, 5), StringField1 FROM TableNumber1;",
                    buildDefaultExecutionConfiguration(),
                    false,
                    this::retrieveChangelogResult,
                    fakeResult(6));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Runtime mode: streaming; Result mode: table
    // --------------------------------------------------------------------------------------------

    @Test
    @Timeout(value = testMethodTimeout, unit = TimeUnit.MILLISECONDS)
    public void testStreamQueryExecutionTable() throws Exception {
        Configuration executionConfiguration = buildDefaultExecutionConfiguration();
        executionConfiguration.set(EXECUTION_RESULT_MODE, ResultMode.TABLE);

        executeQuery(
                "SELECT scalarUDF(IntegerField1, 5), StringField1, 'ABC' FROM TableNumber1;",
                executionConfiguration,
                true,
                this::retrieveTableResult,
                fakeResult(6));
    }

    @Test
    @Timeout(value = 3 * testMethodTimeout, unit = TimeUnit.MILLISECONDS)
    public void testStreamQueryExecutionTableMultipleTimes() throws Exception {
        Configuration executionConfiguration = buildDefaultExecutionConfiguration();
        executionConfiguration.set(EXECUTION_RESULT_MODE, ResultMode.TABLE);

        for (int i = 0; i < 3; i++) {
            executeQuery(
                    "SELECT scalarUDF(IntegerField1, 5), StringField1 FROM TableNumber1;",
                    executionConfiguration,
                    true,
                    this::retrieveTableResult,
                    fakeResult(6));
        }
    }

    @Test
    @Timeout(value = testMethodTimeout, unit = TimeUnit.MILLISECONDS)
    public void testStreamQueryExecutionLimitedTable() throws Exception {
        Configuration executionConfiguration = buildDefaultExecutionConfiguration();
        executionConfiguration.set(EXECUTION_RESULT_MODE, ResultMode.TABLE);
        executionConfiguration.setInteger(EXECUTION_MAX_TABLE_RESULT_ROWS, 1);

        executeQuery(
                "SELECT COUNT(*), StringField1 FROM TableNumber1 GROUP BY StringField1;",
                executionConfiguration,
                true,
                this::retrieveTableResult,
                fakeResult(1));
    }

    // --------------------------------------------------------------------------------------------
    // Runtime mode: batch; Result mode: table
    // --------------------------------------------------------------------------------------------

    @Test
    @Timeout(value = testMethodTimeout, unit = TimeUnit.MILLISECONDS)
    public void testBatchQueryExecution() throws Exception {
        // Currently, the 'execution.runtime-mode' can only be set when instantiating the table
        // environment, So store the current environment and  switch to batch mode.
        RemoteExecutor oldExecutor = executor;
        SessionHandle oldHandle = sessionHandle;
        MiniClusterExtension miniClusterExtension =
                new MiniClusterExtension(
                        () -> {
                            Configuration configuration = new Configuration();
                            configuration.set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);
                            return new MiniClusterResourceConfiguration.Builder()
                                    .setConfiguration(configuration)
                                    .build();
                        });
        miniClusterExtension.beforeAll(null);
        SqlGatewayServiceExtension sqlGatewayServiceExtension =
                new SqlGatewayServiceExtension(miniClusterExtension::getClientConfiguration);
        sqlGatewayServiceExtension.beforeAll(null);
        SqlGatewayRestEndpointExtension sqlGatewayRestEndpointExtension =
                new SqlGatewayRestEndpointExtension(sqlGatewayServiceExtension::getService);
        sqlGatewayRestEndpointExtension.beforeAll(null);
        executor =
                new RemoteExecutor(
                        miniClusterExtension.getClientConfiguration(),
                        sqlGatewayRestEndpointExtension.getTargetAddress(),
                        sqlGatewayRestEndpointExtension.getTargetPort());
        executor.start();
        sessionHandle = executor.getSessionHandle();

        URL url = RemoteExecutorITCase.class.getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
        initSession(replaceVars);

        Configuration executionConfiguration = buildDefaultExecutionConfiguration();
        executionConfiguration.set(EXECUTION_RESULT_MODE, ResultMode.TABLE);

        executeQuery(
                "SELECT *, 'ABC' FROM TestView1;",
                executionConfiguration,
                true,
                this::retrieveTableResult,
                fakeResult(6));

        // Restore environment for the rest tests
        miniClusterExtension.afterAll(null);
        sqlGatewayServiceExtension.afterAll(null);
        sqlGatewayRestEndpointExtension.afterAll(null);
        executor = oldExecutor;
        sessionHandle = oldHandle;
    }

    @Test
    @Timeout(value = 3 * testMethodTimeout, unit = TimeUnit.MILLISECONDS)
    public void testBatchQueryExecutionMultipleTimes() throws Exception {
        RemoteExecutor oldExecutor = executor;
        SessionHandle oldHandle = sessionHandle;
        MiniClusterExtension miniClusterExtension =
                new MiniClusterExtension(
                        () -> {
                            Configuration configuration = new Configuration();
                            configuration.set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);
                            return new MiniClusterResourceConfiguration.Builder()
                                    .setConfiguration(configuration)
                                    .build();
                        });
        miniClusterExtension.beforeAll(null);
        SqlGatewayServiceExtension sqlGatewayServiceExtension =
                new SqlGatewayServiceExtension(miniClusterExtension::getClientConfiguration);
        sqlGatewayServiceExtension.beforeAll(null);
        SqlGatewayRestEndpointExtension sqlGatewayRestEndpointExtension =
                new SqlGatewayRestEndpointExtension(sqlGatewayServiceExtension::getService);
        sqlGatewayRestEndpointExtension.beforeAll(null);
        executor =
                new RemoteExecutor(
                        miniClusterExtension.getClientConfiguration(),
                        sqlGatewayRestEndpointExtension.getTargetAddress(),
                        sqlGatewayRestEndpointExtension.getTargetPort());
        executor.start();
        sessionHandle = executor.getSessionHandle();

        URL url = RemoteExecutorITCase.class.getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());
        initSession(replaceVars);

        Configuration executionConfiguration = buildDefaultExecutionConfiguration();
        executionConfiguration.set(EXECUTION_RESULT_MODE, ResultMode.TABLE);

        for (int i = 0; i < 3; i++) {
            executeQuery(
                    "SELECT * FROM TestView1;",
                    executionConfiguration,
                    true,
                    this::retrieveTableResult,
                    fakeResult(6));
        }

        miniClusterExtension.afterAll(null);
        sqlGatewayServiceExtension.afterAll(null);
        sqlGatewayRestEndpointExtension.afterAll(null);
        executor = oldExecutor;
        sessionHandle = oldHandle;
    }

    // --------------------------------------------------------------------------------------------
    // Other tests
    // --------------------------------------------------------------------------------------------

    @Test
    @Timeout(value = testMethodTimeout, unit = TimeUnit.MILLISECONDS)
    public void testSetResetProperty() {
        // test set
        executeStatement("SET 'key1' = 'value1';", null);
        executeStatement("SET 'key2' = 'value2';", null);
        assertThat(executor.getSessionConfig())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");

        // test reset one property
        executeStatement("RESET 'key1';", null);
        assertThat(executor.getSessionConfig()).doesNotContainKey("key1");

        // test reset all properties
        executeStatement("SET 'key1' = 'value1';", null);
        assertThat(executor.getSessionConfig())
                .containsEntry("key1", "value1")
                .containsEntry("key2", "value2");
        executeStatement("RESET;", null);
        assertThat(executor.getSessionConfig()).doesNotContainKey("key1").doesNotContainKey("key2");
    }

    @Test
    @Timeout(value = testMethodTimeout, unit = TimeUnit.MILLISECONDS)
    public void testGetSessionConfig() {
        assertThat(executor.getSessionConfig())
                .isEqualTo(
                        SQL_GATEWAY_SERVICE_EXTENSION.getService().getSessionConfig(sessionHandle));
    }

    @Test
    public void testKeepingAlive() throws Exception {
        long lastAccessTime =
                SQL_GATEWAY_SERVICE_EXTENSION
                        .getSessionManager()
                        .getSession(sessionHandle)
                        .getLastAccessTime();

        // Wait to trigger heartbeat
        Thread.sleep(20_000L);

        assertThat(
                        SQL_GATEWAY_SERVICE_EXTENSION
                                        .getSessionManager()
                                        .getSession(sessionHandle)
                                        .getLastAccessTime()
                                > lastAccessTime)
                .isTrue();
    }

    // --------------------------------------------------------------------------------------------

    private void executeQuery(
            String sql,
            Configuration executionConfig,
            boolean isMaterialized,
            BiFunction<String, RowDataToStringConverter, List<String>> resultGetter,
            List<String> expectedResults) {
        ResultDescriptor descriptor = ResultDescriptor.of(executeStatement(sql, executionConfig));

        assertThat(descriptor.isMaterialized()).isEqualTo(isMaterialized);

        List<String> actualResults =
                resultGetter.apply(
                        descriptor.getResultId(), descriptor.getRowDataStringConverter());

        TestBaseUtils.compareResultCollections(
                expectedResults, actualResults, Comparator.naturalOrder());
    }

    private static TableResultWrapper executeStatement(
            String statement, @Nullable Configuration executionConfig) {
        return executor.executeStatement(statement, executionTimeout, executionConfig);
    }

    /** Default execution configuration for changelog result. */
    private Configuration buildDefaultExecutionConfiguration() {
        HashMap<String, String> configMap = new HashMap<>();
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.CHANGELOG.name());
        configMap.put(EXECUTION_MAX_TABLE_RESULT_ROWS.key(), "100");
        return Configuration.fromMap(configMap);
    }

    private static List<String> getInitSQL(final Map<String, String> replaceVars) {
        return Stream.of(
                        String.format(
                                "CREATE FUNCTION scalarUDF AS '%s'",
                                UserDefinedFunctions.ScalarUDF.class.getName()),
                        String.format(
                                "CREATE FUNCTION aggregateUDF AS '%s'",
                                AggregateFunction.class.getName()),
                        String.format(
                                "CREATE FUNCTION tableUDF AS '%s'",
                                UserDefinedFunctions.TableUDF.class.getName()),
                        "CREATE TABLE TableNumber1 (\n"
                                + "  IntegerField1 INT,\n"
                                + "  StringField1 STRING,\n"
                                + "  TimestampField1 TIMESTAMP(3)\n"
                                + ") WITH (\n"
                                + "  'connector' = 'filesystem',\n"
                                + "  'path' = '$VAR_SOURCE_PATH1',\n"
                                + "  'format' = 'csv',\n"
                                + "  'csv.ignore-parse-errors' = 'true',\n"
                                + "  'csv.allow-comments' = 'true'\n"
                                + ")\n",
                        "CREATE VIEW TestView1 AS SELECT scalarUDF(IntegerField1, 5) FROM TableNumber1\n",
                        "CREATE TABLE TableSourceSink (\n"
                                + "  BooleanField BOOLEAN,\n"
                                + "  StringField2 STRING,\n"
                                + "  TimestampField2 TIMESTAMP\n"
                                + ") WITH (\n"
                                + "  'connector' = 'filesystem',\n"
                                + "  'path' = '$VAR_SOURCE_SINK_PATH',\n"
                                + "  'format' = 'csv',\n"
                                + "  'csv.ignore-parse-errors' = 'true',\n"
                                + "  'csv.allow-comments' = 'true'\n"
                                + ")\n",
                        "CREATE VIEW TestView2 AS SELECT * FROM TestView1\n")
                .map(
                        sql -> {
                            for (Map.Entry<String, String> replaceVar : replaceVars.entrySet()) {
                                sql = sql.replace(replaceVar.getKey(), replaceVar.getValue());
                            }
                            return sql;
                        })
                .collect(Collectors.toList());
    }

    private static void initSession(Map<String, String> replaceVars) {
        for (String sql : getInitSQL(replaceVars)) {
            executeStatement(sql, null);
        }
    }

    private List<String> retrieveChangelogResult(
            String resultId, RowDataToStringConverter rowDataToStringConverter) {
        List<String> actualResults = new ArrayList<>();
        while (true) {
            try {
                Thread.sleep(50); // slow the processing down
            } catch (InterruptedException e) {
                throw new SqlClientException(e);
            }
            TypedResult<List<RowData>> result = executor.retrieveResultChanges(resultId);
            if (result.getType() == TypedResult.ResultType.PAYLOAD) {
                for (RowData row : result.getPayload()) {
                    actualResults.add(
                            StringUtils.arrayAwareToString(rowDataToStringConverter.convert(row)));
                }
            } else if (result.getType() == TypedResult.ResultType.EOS) {
                break;
            }
        }
        return actualResults;
    }

    private List<String> retrieveTableResult(
            String resultId, RowDataToStringConverter rowDataToStringConverter) {
        List<String> actualResults = new ArrayList<>();
        while (true) {
            try {
                Thread.sleep(50); // slow the processing down
            } catch (InterruptedException e) {
                throw new SqlClientException(e);
            }
            TypedResult<Integer> result = executor.snapshotResult(resultId, 2);
            if (result.getType() == TypedResult.ResultType.PAYLOAD) {
                actualResults.clear();
                IntStream.rangeClosed(1, result.getPayload())
                        .forEach(
                                (page) -> {
                                    for (RowData row :
                                            executor.retrieveResultPage(resultId, page)) {
                                        actualResults.add(
                                                StringUtils.arrayAwareToString(
                                                        rowDataToStringConverter.convert(row)));
                                    }
                                });
            } else if (result.getType() == TypedResult.ResultType.EOS) {
                break;
            }
        }

        return actualResults;
    }

    // TODO: remove this after implementing TableResultWrapper#getRowDataToStringConverter
    private List<String> fakeResult(int repeat) {
        List<String> fake = new ArrayList<>();
        for (int i = 0; i < repeat; i++) {
            fake.add("[FAKE TEST RETURN]");
        }
        return fake;
    }
}
