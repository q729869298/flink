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

package org.apache.flink.connector.pulsar.source.reader.deserializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.util.Collector;

import org.apache.pulsar.client.api.Message;

/**
 * A {@link PulsarDeserializationSchema} implementation which based on the given flink's {@link
 * DeserializationSchema}. We would consume the message as byte array from pulsar and deserialize it
 * by using flink serialization logic.
 *
 * @param <T> The output type of the message.
 */
@Internal
class PulsarDeserializationSchemaWrapper<T> implements PulsarDeserializationSchema<T> {
    private static final long serialVersionUID = -630646912412751300L;

    private final DeserializationSchema<T> deserializationSchema;

    public PulsarDeserializationSchemaWrapper(DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(InitializationContext context, SourceConfiguration configuration)
            throws Exception {
        // Initialize it for some custom logic.
        deserializationSchema.open(context);
    }

    @Override
    public void deserialize(Message<byte[]> message, Collector<T> out) throws Exception {
        byte[] bytes = message.getData();
        T instance = deserializationSchema.deserialize(bytes);

        out.collect(instance);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
