/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.rabbitmq;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link RMQSink}.
 */
public class RMQSinkFeatureTest {

	private static final String EXCHANGE = "exchange";
	private static final String ROUTING_KEY = "application.component.error";
	private static final String EXPIRATION = "10000";
	private static final String MESSAGE_STR = "msg";
	private static final byte[] MESSAGE = new byte[1];

	private RMQConnectionConfig rmqConnectionConfig;
	private ConnectionFactory connectionFactory;
	private Connection connection;
	private Channel channel;
	private SerializationSchema<String> serializationSchema;
	private DummyPublishOptions publishOptions;

	@Before
	public void before() throws Exception {
		serializationSchema = spy(new DummySerializationSchema());
		rmqConnectionConfig = mock(RMQConnectionConfig.class);
		connectionFactory = mock(ConnectionFactory.class);
		connection = mock(Connection.class);
		channel = mock(Channel.class);

		when(rmqConnectionConfig.getConnectionFactory()).thenReturn(connectionFactory);
		when(connectionFactory.newConnection()).thenReturn(connection);
		when(connection.createChannel()).thenReturn(channel);
	}

	@Test
	public void openCallDeclaresQueue() throws Exception {
		createRMQSink();
	}

	@Test
	public void throwExceptionIfChannelIsNull() throws Exception {
		when(connection.createChannel()).thenReturn(null);
		try {
			createRMQSink();
		} catch (RuntimeException ex) {
			assertEquals("None of RabbitMQ channels are available", ex.getMessage());
		}
	}

	private RMQSink<String> createRMQSink() throws Exception {
		publishOptions = new DummyPublishOptions(EXCHANGE,ROUTING_KEY,EXPIRATION);
		RMQSink<String> rmqSink = new RMQSink<String>(rmqConnectionConfig, serializationSchema, publishOptions);
		rmqSink.open(new Configuration());
		return rmqSink;
	}

	@Test
	public void invokePublishBytesToQueue() throws Exception {
		RMQSink<String> rmqSink = createRMQSink();

		rmqSink.invoke(MESSAGE_STR, SinkContextUtil.forTimestamp(0));
		verify(serializationSchema).serialize(MESSAGE_STR);
		verify(channel).basicPublish(publishOptions.computeExchange(""),
				publishOptions.computeRoutingKey(""), publishOptions.computeProperties(""), MESSAGE);
	}

	@Test(expected = RuntimeException.class)
	public void exceptionDuringPublishingIsNotIgnored() throws Exception {
		RMQSink<String> rmqSink = createRMQSink();

		doThrow(IOException.class).when(channel).basicPublish(publishOptions.computeExchange(""),
				publishOptions.computeRoutingKey(""), publishOptions.computeProperties(""), MESSAGE);
		rmqSink.invoke("msg", SinkContextUtil.forTimestamp(0));
	}

	@Test
	public void exceptionDuringPublishingIsIgnoredIfLogFailuresOnly() throws Exception {
		RMQSink<String> rmqSink = createRMQSink();
		rmqSink.setLogFailuresOnly(true);

		doThrow(IOException.class).when(channel).basicPublish(publishOptions.computeExchange(""),
				publishOptions.computeRoutingKey(""), publishOptions.computeProperties(""), MESSAGE);
		rmqSink.invoke("msg", SinkContextUtil.forTimestamp(0));
	}

	@Test
	public void closeAllResources() throws Exception {
		RMQSink<String> rmqSink = createRMQSink();

		rmqSink.close();

		verify(channel).close();
		verify(connection).close();
	}
	
	private class DummyPublishOptions implements RMQSinkPublishOptions<String> {
		
		private String routingKey;
		private String exchange;
		private String expiration;
		
		public DummyPublishOptions(String exchange,String routingKey,String expiration) {
			this.exchange = exchange;
			this.routingKey = routingKey;
			this.expiration = expiration;
		}

		@Override
		public String computeRoutingKey(String a) {
			return routingKey;
		}

		@Override
		public BasicProperties computeProperties(String a) {
			Map<String,Object> headers = new HashMap<String,Object>();
			headers.put("Test", new String("My Value"));
			return new AMQP.BasicProperties.Builder()
					   .headers(headers)
		               .expiration(expiration)
		               .build();
		}

		@Override
		public String computeExchange(String a) {
			return exchange;
		}
		
	}

	private class DummySerializationSchema implements SerializationSchema<String> {
		private static final long serialVersionUID = 1L;

		@Override
		public byte[] serialize(String element) {
			return MESSAGE;
		}
	}
}
