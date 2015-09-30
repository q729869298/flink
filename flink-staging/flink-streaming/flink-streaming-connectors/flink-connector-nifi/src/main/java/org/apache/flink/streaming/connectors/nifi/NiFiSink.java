/**
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
package org.apache.flink.streaming.connectors.nifi;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A sink that delivers data to Apache NiFi using the NiFi Site-to-Site client. The sink requires
 * a NiFiDataPacketBuilder which can create instances of NiFiDataPacket from the incoming data.
 */
public class NiFiSink<T> extends RichSinkFunction<T> {

	private static final Logger LOG = LoggerFactory.getLogger(NiFiSink.class);

	private SiteToSiteClient client;
	private SiteToSiteClientConfig clientConfig;
	private NiFiDataPacketBuilder<T> builder;

	public NiFiSink(SiteToSiteClientConfig clientConfig, NiFiDataPacketBuilder<T> builder) {
		this.clientConfig = clientConfig;
		this.builder = builder;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.client = new SiteToSiteClient.Builder().fromConfig(clientConfig).build();
	}

	@Override
	public void invoke(T value) throws Exception {
		final NiFiDataPacket niFiDataPacket = builder.createNiFiDataPacket(value, getRuntimeContext());

		final Transaction transaction = client.createTransaction(TransferDirection.SEND);
		transaction.send(niFiDataPacket.getContent(), niFiDataPacket.getAttributes());
		transaction.confirm();
		transaction.complete();
	}

	@Override
	public void close() throws Exception {
		super.close();
		try {
			client.close();
		} catch (final IOException ioe) {
			LOG.error("Unable to close SiteToSiteClient: " + ioe.getMessage());
		}
	}

}
