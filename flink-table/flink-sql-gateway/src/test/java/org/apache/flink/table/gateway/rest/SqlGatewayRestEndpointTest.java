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

package org.apache.flink.table.gateway.rest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactoryUtils;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointFactory;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestOptions;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Test;

import static org.apache.flink.table.gateway.api.endpoint.SqlGatewayEndpointFactoryUtils.getEndpointConfig;
import static org.apache.flink.table.gateway.rest.SqlGatewayRestEndpointITCase.getBaseConfig;
import static org.apache.flink.table.gateway.rest.SqlGatewayRestEndpointITCase.getSqlGatewayRestOptionFullName;
import static org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointFactory.IDENTIFIER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for the {@link SqlGatewayRestEndpoint}. */
public class SqlGatewayRestEndpointTest extends TestLogger {

    private static final String ADDRESS1 = "123.123.123.123";
    private static final String ADDRESS2 = "123.123.123.144";
    private static final String BIND_ADDRESS1 = "023.023.023.023";
    private static final String BIND_ADDRESS2 = "023.023.023.333";
    private static final String BIND_PORT1 = "7282";
    private static final String BIND_PORT2 = "7444";
    private static final String PORT1 = "7661";
    private static final String PORT2 = "7662";
    public static final String SQL_GATEWAY_ADDRESS =
            getSqlGatewayRestOptionFullName(SqlGatewayRestOptions.ADDRESS.key());
    public static final String SQL_GATEWAY_BIND_ADDRESS =
            getSqlGatewayRestOptionFullName(SqlGatewayRestOptions.BIND_ADDRESS.key());
    public static final String SQL_GATEWAY_BIND_PORT =
            getSqlGatewayRestOptionFullName(SqlGatewayRestOptions.BIND_PORT.key());
    public static final String SQL_GATEWAY_PORT =
            getSqlGatewayRestOptionFullName(SqlGatewayRestOptions.PORT.key());

    /**
     * Test {@link SqlGatewayRestEndpoint} uses its own options when there are both runtime options
     * and sql gateway options in the delegating configuration.
     */
    @Test
    public void testIfSqlGatewayRestEndpointUseOverrideOptions() throws ConfigurationException {
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString(RestOptions.ADDRESS.key(), ADDRESS1);
        flinkConfig.setString(RestOptions.BIND_ADDRESS.key(), BIND_ADDRESS1);
        flinkConfig.setString(RestOptions.BIND_PORT.key(), BIND_PORT1);
        flinkConfig.setString(RestOptions.PORT.key(), PORT1);

        flinkConfig.setString(SQL_GATEWAY_ADDRESS, ADDRESS2);
        flinkConfig.setString(SQL_GATEWAY_BIND_ADDRESS, BIND_ADDRESS2);
        flinkConfig.setString(SQL_GATEWAY_BIND_PORT, BIND_PORT2);
        flinkConfig.setString(SQL_GATEWAY_PORT, PORT2);

        Configuration sqlGatewayRestEndpointConfig = getBaseConfig(flinkConfig);

        final RestServerEndpointConfiguration result =
                RestServerEndpointConfiguration.fromConfiguration(sqlGatewayRestEndpointConfig);

        assertEquals(ADDRESS2, result.getRestAddress());
        assertEquals(BIND_ADDRESS2, result.getRestBindAddress());
        assertEquals(BIND_PORT2, result.getRestBindPortRange());
    }

    /** Test {@link SqlGatewayRestEndpoint} uses fallback options correctly. */
    @Test
    public void testFallbackOptions() throws ConfigurationException {
        Configuration flinkConfig = new Configuration();

        // Test bind-address fallback to address
        flinkConfig.setString(SQL_GATEWAY_ADDRESS, ADDRESS2);
        RestServerEndpointConfiguration result1 =
                RestServerEndpointConfiguration.fromConfiguration(getBaseConfig(flinkConfig));
        assertEquals(ADDRESS2, result1.getRestAddress());
        assertEquals(ADDRESS2, result1.getRestBindAddress());

        // Test bind-port get the default value
        assertEquals("8083", result1.getRestBindPortRange());

        // Test bind-port fallback to port
        flinkConfig.setString(SQL_GATEWAY_PORT, PORT2);
        result1 = RestServerEndpointConfiguration.fromConfiguration(getBaseConfig(flinkConfig));
        assertEquals(PORT2, result1.getRestBindPortRange());
    }

    /** Test {@link SqlGatewayRestEndpoint} uses required options correctly. */
    @Test
    public void testRequiredOptions() throws ConfigurationException {
        // Empty options
        Configuration flinkConfig1 = new Configuration();
        SqlGatewayEndpointFactoryUtils.DefaultEndpointFactoryContext context =
                new SqlGatewayEndpointFactoryUtils.DefaultEndpointFactoryContext(
                        null, flinkConfig1, getEndpointConfig(flinkConfig1, IDENTIFIER));
        SqlGatewayEndpointFactoryUtils.EndpointFactoryHelper endpointFactoryHelper =
                SqlGatewayEndpointFactoryUtils.createEndpointFactoryHelper(
                        new SqlGatewayRestEndpointFactory(), context);
        assertThrows(ValidationException.class, endpointFactoryHelper::validate);

        // Only ADDRESS
        flinkConfig1.setString(SQL_GATEWAY_ADDRESS, ADDRESS2);
        RestServerEndpointConfiguration result =
                RestServerEndpointConfiguration.fromConfiguration(getBaseConfig(flinkConfig1));
        assertEquals(ADDRESS2, result.getRestAddress());

        // Only BIND PORT
        Configuration flinkConfig2 = new Configuration();
        flinkConfig2.setString(SQL_GATEWAY_BIND_PORT, BIND_PORT2);
        context =
                new SqlGatewayEndpointFactoryUtils.DefaultEndpointFactoryContext(
                        null, flinkConfig2, getEndpointConfig(flinkConfig2, IDENTIFIER));
        endpointFactoryHelper =
                SqlGatewayEndpointFactoryUtils.createEndpointFactoryHelper(
                        new SqlGatewayRestEndpointFactory(), context);
        assertThrows(ValidationException.class, endpointFactoryHelper::validate);

        // Only PORT
        Configuration flinkConfig3 = new Configuration();
        flinkConfig3.setString(SQL_GATEWAY_PORT, PORT2);
        context =
                new SqlGatewayEndpointFactoryUtils.DefaultEndpointFactoryContext(
                        null, flinkConfig3, getEndpointConfig(flinkConfig3, IDENTIFIER));
        endpointFactoryHelper =
                SqlGatewayEndpointFactoryUtils.createEndpointFactoryHelper(
                        new SqlGatewayRestEndpointFactory(), context);
        assertThrows(ValidationException.class, endpointFactoryHelper::validate);

        // ADDRESS and PORT
        flinkConfig1.setString(SQL_GATEWAY_PORT, PORT2);
        result = RestServerEndpointConfiguration.fromConfiguration(getBaseConfig(flinkConfig1));
        assertEquals(ADDRESS2, result.getRestAddress());
        assertEquals(PORT2, result.getRestBindPortRange());

        // ADDRESS and PORT and BIND PORT
        flinkConfig1.setString(SQL_GATEWAY_BIND_PORT, BIND_PORT2);
        result = RestServerEndpointConfiguration.fromConfiguration(getBaseConfig(flinkConfig1));
        assertEquals(ADDRESS2, result.getRestAddress());
        assertEquals(BIND_PORT2, result.getRestBindPortRange());

        // ADDRESS and BIND PORT
        Configuration flinkConfig4 = new Configuration();
        flinkConfig4.setString(SQL_GATEWAY_ADDRESS, ADDRESS2);
        flinkConfig4.setString(SQL_GATEWAY_BIND_PORT, BIND_PORT2);
        result = RestServerEndpointConfiguration.fromConfiguration(getBaseConfig(flinkConfig1));
        assertEquals(ADDRESS2, result.getRestAddress());
        assertEquals(BIND_PORT2, result.getRestBindPortRange());
    }
}
