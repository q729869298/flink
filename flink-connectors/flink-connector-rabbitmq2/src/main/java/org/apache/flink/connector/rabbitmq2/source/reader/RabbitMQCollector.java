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

package org.apache.flink.connector.rabbitmq2.source.reader;

import org.apache.flink.connector.rabbitmq2.source.common.RabbitMQSourceMessageWrapper;
import org.apache.flink.util.Collector;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The collector for the messages received from RabbitMQ. Deserialized receive their identifiers
 * through {@link #setMessageIdentifiers(String, long)} before they are collected through {@link
 * #collect(Object)}. Messages can be polled in order to be processed by the output.
 *
 * @param <T> The output type of the source.
 * @see RabbitMQSourceMessageWrapper
 */
public class RabbitMQCollector<T> implements Collector<T> {
    // Queue that holds the messages.
    private final BlockingQueue<RabbitMQSourceMessageWrapper<T>> unpolledMessageQueue;
    // Identifiers of the next message that will be collected.
    private long deliveryTag;
    private String correlationId;

    private RabbitMQCollector(int capacity) {
        this.unpolledMessageQueue = new LinkedBlockingQueue<>(capacity);
    }

    public RabbitMQCollector() {
        this(Integer.MAX_VALUE);
    }

    /** @return boolean true if there are messages remaining in the collector. */
    public boolean hasUnpolledMessages() {
        return !unpolledMessageQueue.isEmpty();
    }

    /**
     * Poll a message from the collector.
     *
     * @return Message the polled message.
     */
    public RabbitMQSourceMessageWrapper<T> pollMessage() {
        return unpolledMessageQueue.poll();
    }

    /**
     * Sets the correlation id and the delivery tag that corresponds to the records originating from
     * the RMQ event. If the correlation id has been processed before, records will not be emitted
     * downstream.
     *
     * <p>If not set explicitly, the {@link AMQP.BasicProperties#getCorrelationId()} and {@link
     * Envelope#getDeliveryTag()} will be used.
     */
    public void setMessageIdentifiers(String correlationId, long deliveryTag) {
        this.correlationId = correlationId;
        this.deliveryTag = deliveryTag;
    }

    @Override
    public void collect(T record) {
        unpolledMessageQueue.add(
                new RabbitMQSourceMessageWrapper<>(deliveryTag, correlationId, record));
    }

    @Override
    public void close() {}
}
