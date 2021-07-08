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

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** Specialized serializer for {@code ShortValueArray}. */
public final class ShortValueArraySerializer extends TypeSerializerSingleton<ShortValueArray> {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public ShortValueArray createInstance() {
        return new ShortValueArray();
    }

    @Override
    public ShortValueArray copy(ShortValueArray from) {
        return copy(from, new ShortValueArray());
    }

    @Override
    public ShortValueArray copy(ShortValueArray from, ShortValueArray reuse) {
        reuse.setValue(from);
        return reuse;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(ShortValueArray record, DataOutputView target) throws IOException {
        record.write(target);
    }

    @Override
    public ShortValueArray deserialize(DataInputView source) throws IOException {
        return deserialize(new ShortValueArray(), source);
    }

    @Override
    public ShortValueArray deserialize(ShortValueArray reuse, DataInputView source)
            throws IOException {
        reuse.read(source);
        return reuse;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        ShortValueArray.copyInternal(source, target);
    }

    // ------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<ShortValueArray> snapshotConfiguration() {
        return new ShortValueArraySerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class ShortValueArraySerializerSnapshot
            extends SimpleTypeSerializerSnapshot<ShortValueArray> {

        public ShortValueArraySerializerSnapshot() {
            super(ShortValueArraySerializer::new);
        }
    }
}
