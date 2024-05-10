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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.runtime.asyncprocessing.EpochManager.Epoch;
import org.apache.flink.runtime.state.v2.InternalPartitionedState;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * A context that preserves the necessary variables required by each operation, all operations for
 * one record will share the same element context.
 *
 * <p>Reference counting mechanism, please refer to {@link ContextStateFutureImpl}.
 *
 * @param <K> The type of the key inside the record.
 */
public class RecordContext<K> extends ReferenceCounted<RecordContext.DisposerRunner> {
    /** The empty record for timer and non-record input usage. */
    static final Object EMPTY_RECORD = new Object();

    /** The record to be processed. */
    private final Object record;

    /** The key inside the record. */
    private final K key;

    /** Whether this Record(Context) has occupied the corresponding key. */
    private volatile boolean keyOccupied;

    /**
     * The disposer for disposing this context. This should be invoked in {@link
     * #referenceCountReachedZero}, which may be called once the ref count reaches zero in any
     * thread.
     */
    private final Consumer<RecordContext<K>> disposer;

    /** The keyGroup to which key belongs. */
    private final int keyGroup;

    /** The namespaces of states. Lazy initialization for saving memory. */
    private Map<InternalPartitionedState<?>, Object> namespaces = null;

    private final ArrayList<AtomicReference<?>> declaredVariables;

    /**
     * The extra context info which is used to hold customized data defined by state backend. The
     * state backend can use this field to cache some data that can be used multiple times in
     * different stages of asynchronous state execution.
     */
    private @Nullable volatile Object extra;

    /** The epoch of this context. */
    private final Epoch epoch;

    public RecordContext(
            Object record,
            K key,
            Consumer<RecordContext<K>> disposer,
            int keyGroup,
            Epoch epoch,
            int variableCount) {
        super(0);
        this.record = record;
        this.key = key;
        this.keyOccupied = false;
        this.disposer = disposer;
        this.keyGroup = keyGroup;
        this.epoch = epoch;
        this.declaredVariables = new ArrayList<>(variableCount);
        for (int i = 0; i < variableCount; i++) {
            declaredVariables.add(null);
        }
    }

    public Object getRecord() {
        return record;
    }

    public K getKey() {
        return this.key;
    }

    /** Check if this context has occupied the key. */
    boolean isKeyOccupied() {
        return keyOccupied;
    }

    /** Set the flag that marks this context has occupied the corresponding key. */
    void setKeyOccupied() {
        keyOccupied = true;
    }

    @Override
    protected void referenceCountReachedZero(@Nullable DisposerRunner disposerRunner) {
        if (keyOccupied) {
            keyOccupied = false;
            if (disposerRunner != null) {
                disposerRunner.runDisposer(() -> disposer.accept(this));
            } else {
                disposer.accept(this);
            }
        }
    }

    public int getKeyGroup() {
        return keyGroup;
    }

    @SuppressWarnings("unchecked")
    public <N> N getNamespace(InternalPartitionedState<N> state) {
        return namespaces == null ? null : (N) namespaces.get(state);
    }

    public <N> void setNamespace(InternalPartitionedState<N> state, N namespace) {
        if (namespaces == null) {
            namespaces = new HashMap<>();
        }
        namespaces.put(state, namespace);
    }

    public AtomicReference<?> getVariableReference(int i) {
        return i >= declaredVariables.size() ? null : declaredVariables.get(i);
    }

    public void setVariableReference(int i, AtomicReference<?> reference) {
        declaredVariables.set(i, reference);
    }

    public void setExtra(Object extra) {
        this.extra = extra;
    }

    public Object getExtra() {
        return extra;
    }

    public Epoch getEpoch() {
        return epoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(record, key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordContext<?> that = (RecordContext<?>) o;
        if (!Objects.equals(record, that.record)) {
            return false;
        }
        if (!Objects.equals(keyGroup, that.keyGroup)) {
            return false;
        }
        if (!Objects.equals(epoch, that.epoch)) {
            return false;
        }
        return Objects.equals(key, that.key);
    }

    @Override
    public String toString() {
        return "RecordContext{"
                + "record="
                + record
                + ", key="
                + key
                + ", occupied="
                + keyOccupied
                + ", ref="
                + getReferenceCount()
                + ", epoch="
                + epoch.id
                + "}";
    }

    public interface DisposerRunner {
        void runDisposer(Runnable task);
    }
}
