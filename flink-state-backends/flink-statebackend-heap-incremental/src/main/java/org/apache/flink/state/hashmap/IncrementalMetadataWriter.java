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

package org.apache.flink.state.hashmap;

import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapSnapshotStrategy;

import java.io.IOException;
import java.util.Objects;

class IncrementalMetadataWriter<K> implements HeapSnapshotStrategy.MetadataWriter {

    private final IncrementalKeyedBackendSerializationProxy<K> serializationProxy;

    public IncrementalMetadataWriter(
            IncrementalHeapSnapshotResources<K> resources, StreamCompressionDecorator decorator) {
        serializationProxy =
                new IncrementalKeyedBackendSerializationProxy<>(
                        resources.getKeySerializer(),
                        resources.getMetaInfoSnapshots(),
                        !Objects.equals(
                                UncompressedStreamCompressionDecorator.INSTANCE, decorator));
    }

    @Override
    public void write(DataOutputViewStreamWrapper outView) throws IOException {
        serializationProxy.write(outView);
    }
}
