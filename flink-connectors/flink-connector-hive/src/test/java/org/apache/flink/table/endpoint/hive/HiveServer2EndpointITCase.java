/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.endpoint.hive;

import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.endpoint.hive.util.HiveServer2EndpointExtension;
import org.apache.flink.table.endpoint.hive.util.ThriftObjectConversions;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.service.session.SessionManager;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;

import org.apache.hadoop.hive.common.auth.HiveAuthUtils;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TCloseSessionReq;
import org.apache.hive.service.rpc.thrift.TCloseSessionResp;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.hive.service.rpc.thrift.TStatusCode;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.AbstractMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** ITCase for {@link HiveServer2Endpoint}. */
public class HiveServer2EndpointITCase {

    @RegisterExtension
    @Order(1)
    public static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension();

    @RegisterExtension
    @Order(2)
    public static final HiveServer2EndpointExtension ENDPOINT_EXTENSION =
            new HiveServer2EndpointExtension(SQL_GATEWAY_SERVICE_EXTENSION::getService);

    @Test
    public void testOpenCloseJdbcConnection() throws Exception {
        SessionManager sessionManager = SQL_GATEWAY_SERVICE_EXTENSION.getSessionManager();
        int originSessionCount = sessionManager.currentSessionCount();
        try (Connection ignore = getConnection()) {
            assertEquals(1 + originSessionCount, sessionManager.currentSessionCount());
        }
        assertEquals(originSessionCount, sessionManager.currentSessionCount());
    }

    @Test
    public void testOpenSessionWithConfig() throws Exception {
        TCLIService.Client client = createClient();
        TOpenSessionReq openSessionReq = new TOpenSessionReq();
        TOpenSessionResp openSessionResp = client.OpenSession(openSessionReq);
        SessionHandle sessionHandle =
                ThriftObjectConversions.toSessionHandle(openSessionResp.getSessionHandle());

        Map<String, String> actualConfig =
                SQL_GATEWAY_SERVICE_EXTENSION.getService().getSessionConfig(sessionHandle);
        assertThat(
                actualConfig.entrySet(),
                hasItem(
                        new AbstractMap.SimpleEntry<>(
                                TableConfigOptions.TABLE_SQL_DIALECT.key(),
                                SqlDialect.HIVE.name())));
    }

    @Test
    public void testGetException() throws Exception {
        TCLIService.Client client = createClient();
        TCloseSessionReq closeSessionReq = new TCloseSessionReq();
        SessionHandle sessionHandle = SessionHandle.create();
        closeSessionReq.setSessionHandle(ThriftObjectConversions.toTSessionHandle(sessionHandle));
        TCloseSessionResp resp = client.CloseSession(closeSessionReq);
        assertEquals(TStatusCode.ERROR_STATUS, resp.getStatus().getStatusCode());
        assertTrue(
                resp.getStatus()
                        .getErrorMessage()
                        .contains(String.format("Session '%s' does not exist", sessionHandle)));
    }

    private Connection getConnection() throws Exception {
        return DriverManager.getConnection(
                String.format(
                        "jdbc:hive2://localhost:%s/default;auth=noSasl",
                        ENDPOINT_EXTENSION.getPort()));
    }

    private TCLIService.Client createClient() throws Exception {
        TTransport transport =
                HiveAuthUtils.getSocketTransport("localhost", ENDPOINT_EXTENSION.getPort(), 0);
        transport.open();
        return new TCLIService.Client(new TBinaryProtocol(transport));
    }
}
