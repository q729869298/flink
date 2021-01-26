/*
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

package org.apache.flink.contrib.streaming.state.snapshot;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.contrib.streaming.state.iterator.RocksStatesPerKeyGroupMergeIterator;
import org.apache.flink.contrib.streaming.state.iterator.RocksTransformingIteratorWrapper;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.FullSnapshotResources;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.function.SupplierWithException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.END_OF_KEY_GROUP_MARK;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.hasMetaDataFollowsFlag;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.setMetaDataFollowsFlagInKey;

/**
 * Snapshot strategy to create full snapshots of {@link
 * org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend}. Iterates and writes all
 * states from a RocksDB snapshot of the column families.
 *
 * @param <K> type of the backend keys.
 */
public class RocksFullSnapshotStrategy<K>
        extends RocksDBSnapshotStrategyBase<K, FullSnapshotResources> {

    private static final Logger LOG = LoggerFactory.getLogger(RocksFullSnapshotStrategy.class);

    private static final String DESCRIPTION = "Asynchronous full RocksDB snapshot";

    /** This decorator is used to apply compression per key-group for the written snapshot data. */
    @Nonnull private final StreamCompressionDecorator keyGroupCompressionDecorator;

    public RocksFullSnapshotStrategy(
            @Nonnull RocksDB db,
            @Nonnull ResourceGuard rocksDBResourceGuard,
            @Nonnull TypeSerializer<K> keySerializer,
            @Nonnull LinkedHashMap<String, RocksDbKvStateInfo> kvStateInformation,
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnegative int keyGroupPrefixBytes,
            @Nonnull LocalRecoveryConfig localRecoveryConfig,
            @Nonnull StreamCompressionDecorator keyGroupCompressionDecorator) {
        super(
                DESCRIPTION,
                db,
                rocksDBResourceGuard,
                keySerializer,
                kvStateInformation,
                keyGroupRange,
                keyGroupPrefixBytes,
                localRecoveryConfig);

        this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
    }

    @Override
    public FullSnapshotResources syncPrepareResources(long checkpointId) throws Exception {

        final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                new ArrayList<>(kvStateInformation.size());
        final List<RocksDbKvStateInfo> metaDataCopy = new ArrayList<>(kvStateInformation.size());

        for (RocksDbKvStateInfo stateInfo : kvStateInformation.values()) {
            // snapshot meta info
            stateMetaInfoSnapshots.add(stateInfo.metaInfo.snapshot());
            metaDataCopy.add(stateInfo);
        }

        final ResourceGuard.Lease lease = rocksDBResourceGuard.acquireResource();
        final Snapshot snapshot = db.getSnapshot();

        return new FullRocksDBSnapshotResources(
                lease, snapshot, metaDataCopy, stateMetaInfoSnapshots, db, keyGroupPrefixBytes);
    }

    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            FullSnapshotResources fullRocksDBSnapshotResources,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory checkpointStreamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {

        if (fullRocksDBSnapshotResources.getMetaInfoSnapshots().isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Asynchronous RocksDB snapshot performed on empty keyed state at {}. Returning null.",
                        timestamp);
            }
            return registry -> SnapshotResult.empty();
        }

        final SupplierWithException<CheckpointStreamWithResultProvider, Exception>
                checkpointStreamSupplier =
                        createCheckpointStreamSupplier(
                                checkpointId, checkpointStreamFactory, checkpointOptions);

        return new SnapshotAsynchronousPartCallable(
                checkpointStreamSupplier, fullRocksDBSnapshotResources);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // nothing to do.
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        // nothing to do.
    }

    private SupplierWithException<CheckpointStreamWithResultProvider, Exception>
            createCheckpointStreamSupplier(
                    long checkpointId,
                    CheckpointStreamFactory primaryStreamFactory,
                    CheckpointOptions checkpointOptions) {

        return localRecoveryConfig.isLocalRecoveryEnabled()
                        && !checkpointOptions.getCheckpointType().isSavepoint()
                ? () ->
                        CheckpointStreamWithResultProvider.createDuplicatingStream(
                                checkpointId,
                                CheckpointedStateScope.EXCLUSIVE,
                                primaryStreamFactory,
                                localRecoveryConfig.getLocalStateDirectoryProvider())
                : () ->
                        CheckpointStreamWithResultProvider.createSimpleStream(
                                CheckpointedStateScope.EXCLUSIVE, primaryStreamFactory);
    }

    /** Encapsulates the process to perform a full snapshot of a RocksDBKeyedStateBackend. */
    private class SnapshotAsynchronousPartCallable
            implements SnapshotResultSupplier<KeyedStateHandle> {

        /** Supplier for the stream into which we write the snapshot. */
        @Nonnull
        private final SupplierWithException<CheckpointStreamWithResultProvider, Exception>
                checkpointStreamSupplier;

        @Nonnull private final FullSnapshotResources snapshotResources;

        SnapshotAsynchronousPartCallable(
                @Nonnull
                        SupplierWithException<CheckpointStreamWithResultProvider, Exception>
                                checkpointStreamSupplier,
                @Nonnull FullSnapshotResources snapshotResources) {

            this.checkpointStreamSupplier = checkpointStreamSupplier;
            this.snapshotResources = snapshotResources;
        }

        @Override
        public SnapshotResult<KeyedStateHandle> get(CloseableRegistry snapshotCloseableRegistry)
                throws Exception {
            final KeyGroupRangeOffsets keyGroupRangeOffsets =
                    new KeyGroupRangeOffsets(keyGroupRange);
            final CheckpointStreamWithResultProvider checkpointStreamWithResultProvider =
                    checkpointStreamSupplier.get();

            snapshotCloseableRegistry.registerCloseable(checkpointStreamWithResultProvider);
            writeSnapshotToOutputStream(checkpointStreamWithResultProvider, keyGroupRangeOffsets);

            if (snapshotCloseableRegistry.unregisterCloseable(checkpointStreamWithResultProvider)) {
                return CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult(
                        checkpointStreamWithResultProvider.closeAndFinalizeCheckpointStreamResult(),
                        keyGroupRangeOffsets);
            } else {
                throw new IOException("Stream is already unregistered/closed.");
            }
        }

        private void writeSnapshotToOutputStream(
                @Nonnull CheckpointStreamWithResultProvider checkpointStreamWithResultProvider,
                @Nonnull KeyGroupRangeOffsets keyGroupRangeOffsets)
                throws IOException, InterruptedException {

            final DataOutputView outputView =
                    new DataOutputViewStreamWrapper(
                            checkpointStreamWithResultProvider.getCheckpointOutputStream());

            writeKVStateMetaData(outputView);

            try (KeyValueStateIterator kvStateIterator =
                    snapshotResources.createKVStateIterator()) {
                writeKVStateData(
                        kvStateIterator, checkpointStreamWithResultProvider, keyGroupRangeOffsets);
            }
        }

        private void writeKVStateMetaData(final DataOutputView outputView) throws IOException {

            KeyedBackendSerializationProxy<K> serializationProxy =
                    new KeyedBackendSerializationProxy<>(
                            // TODO: this code assumes that writing a serializer is threadsafe, we
                            // should support to
                            // get a serialized form already at state registration time in the
                            // future
                            keySerializer,
                            snapshotResources.getMetaInfoSnapshots(),
                            !Objects.equals(
                                    UncompressedStreamCompressionDecorator.INSTANCE,
                                    keyGroupCompressionDecorator));

            serializationProxy.write(outputView);
        }

        private void writeKVStateData(
                final KeyValueStateIterator mergeIterator,
                final CheckpointStreamWithResultProvider checkpointStreamWithResultProvider,
                final KeyGroupRangeOffsets keyGroupRangeOffsets)
                throws IOException, InterruptedException {

            byte[] previousKey = null;
            byte[] previousValue = null;
            DataOutputView kgOutView = null;
            OutputStream kgOutStream = null;
            CheckpointStreamFactory.CheckpointStateOutputStream checkpointOutputStream =
                    checkpointStreamWithResultProvider.getCheckpointOutputStream();

            try {

                // preamble: setup with first key-group as our lookahead
                if (mergeIterator.isValid()) {
                    // begin first key-group by recording the offset
                    keyGroupRangeOffsets.setKeyGroupOffset(
                            mergeIterator.keyGroup(), checkpointOutputStream.getPos());
                    // write the k/v-state id as metadata
                    kgOutStream =
                            keyGroupCompressionDecorator.decorateWithCompression(
                                    checkpointOutputStream);
                    kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
                    // TODO this could be aware of keyGroupPrefixBytes and write only one byte
                    // if possible
                    kgOutView.writeShort(mergeIterator.kvStateId());
                    previousKey = mergeIterator.key();
                    previousValue = mergeIterator.value();
                    mergeIterator.next();
                }

                // main loop: write k/v pairs ordered by (key-group, kv-state), thereby tracking
                // key-group offsets.
                while (mergeIterator.isValid()) {

                    assert (!hasMetaDataFollowsFlag(previousKey));

                    // set signal in first key byte that meta data will follow in the stream
                    // after this k/v pair
                    if (mergeIterator.isNewKeyGroup() || mergeIterator.isNewKeyValueState()) {

                        // be cooperative and check for interruption from time to time in the
                        // hot loop
                        checkInterrupted();

                        setMetaDataFollowsFlagInKey(previousKey);
                    }

                    writeKeyValuePair(previousKey, previousValue, kgOutView);

                    // write meta data if we have to
                    if (mergeIterator.isNewKeyGroup()) {
                        // TODO this could be aware of keyGroupPrefixBytes and write only one
                        // byte if possible
                        kgOutView.writeShort(END_OF_KEY_GROUP_MARK);
                        // this will just close the outer stream
                        kgOutStream.close();
                        // begin new key-group
                        keyGroupRangeOffsets.setKeyGroupOffset(
                                mergeIterator.keyGroup(), checkpointOutputStream.getPos());
                        // write the kev-state
                        // TODO this could be aware of keyGroupPrefixBytes and write only one
                        // byte if possible
                        kgOutStream =
                                keyGroupCompressionDecorator.decorateWithCompression(
                                        checkpointOutputStream);
                        kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
                        kgOutView.writeShort(mergeIterator.kvStateId());
                    } else if (mergeIterator.isNewKeyValueState()) {
                        // write the k/v-state
                        // TODO this could be aware of keyGroupPrefixBytes and write only one
                        // byte if possible
                        kgOutView.writeShort(mergeIterator.kvStateId());
                    }

                    // request next k/v pair
                    previousKey = mergeIterator.key();
                    previousValue = mergeIterator.value();
                    mergeIterator.next();
                }

                // epilogue: write last key-group
                if (previousKey != null) {
                    assert (!hasMetaDataFollowsFlag(previousKey));
                    setMetaDataFollowsFlagInKey(previousKey);
                    writeKeyValuePair(previousKey, previousValue, kgOutView);
                    // TODO this could be aware of keyGroupPrefixBytes and write only one byte if
                    // possible
                    kgOutView.writeShort(END_OF_KEY_GROUP_MARK);
                    // this will just close the outer stream
                    kgOutStream.close();
                    kgOutStream = null;
                }

            } finally {
                // this will just close the outer stream
                IOUtils.closeQuietly(kgOutStream);
            }
        }

        private void writeKeyValuePair(byte[] key, byte[] value, DataOutputView out)
                throws IOException {
            BytePrimitiveArraySerializer.INSTANCE.serialize(key, out);
            BytePrimitiveArraySerializer.INSTANCE.serialize(value, out);
        }

        private void checkInterrupted() throws InterruptedException {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("RocksDB snapshot interrupted.");
            }
        }
    }

    static class FullRocksDBSnapshotResources implements FullSnapshotResources {
        private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;
        private final ResourceGuard.Lease lease;
        private final Snapshot snapshot;
        private final RocksDB db;
        private final List<MetaData> metaData;

        /** Number of bytes in the key-group prefix. */
        @Nonnegative private final int keyGroupPrefixBytes;

        public FullRocksDBSnapshotResources(
                ResourceGuard.Lease lease,
                Snapshot snapshot,
                List<RocksDbKvStateInfo> metaDataCopy,
                List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
                RocksDB db,
                int keyGroupPrefixBytes) {
            this.lease = lease;
            this.snapshot = snapshot;
            this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
            this.db = db;
            this.keyGroupPrefixBytes = keyGroupPrefixBytes;

            // we need to to this in the constructor, i.e. in the synchronous part of the snapshot
            // TODO: better yet, we can do it outside the constructor
            this.metaData = fillMetaData(metaDataCopy);
        }

        private List<MetaData> fillMetaData(List<RocksDbKvStateInfo> metaDataCopy) {
            List<MetaData> metaData = new ArrayList<>(metaDataCopy.size());
            for (RocksDbKvStateInfo rocksDbKvStateInfo : metaDataCopy) {
                StateSnapshotTransformer<byte[]> stateSnapshotTransformer = null;
                if (rocksDbKvStateInfo.metaInfo instanceof RegisteredKeyValueStateBackendMetaInfo) {
                    stateSnapshotTransformer =
                            ((RegisteredKeyValueStateBackendMetaInfo<?, ?>)
                                            rocksDbKvStateInfo.metaInfo)
                                    .getStateSnapshotTransformFactory()
                                    .createForSerializedState()
                                    .orElse(null);
                }
                metaData.add(new MetaData(rocksDbKvStateInfo, stateSnapshotTransformer));
            }
            return metaData;
        }

        @Override
        public KeyValueStateIterator createKVStateIterator() throws IOException {
            CloseableRegistry closeableRegistry = new CloseableRegistry();

            try {
                ReadOptions readOptions = new ReadOptions();
                closeableRegistry.registerCloseable(readOptions::close);
                readOptions.setSnapshot(snapshot);

                List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators =
                        createKVStateIterators(closeableRegistry, readOptions);

                // Here we transfer ownership of the required resources to the
                // RocksStatesPerKeyGroupMergeIterator
                return new RocksStatesPerKeyGroupMergeIterator(
                        closeableRegistry, new ArrayList<>(kvStateIterators), keyGroupPrefixBytes);
            } catch (Throwable t) {
                // If anything goes wrong, clean up our stuff. If things went smoothly the
                // merging iterator is now responsible for closing the resources
                IOUtils.closeQuietly(closeableRegistry);
                throw new IOException("Error creating merge iterator", t);
            }
        }

        private List<Tuple2<RocksIteratorWrapper, Integer>> createKVStateIterators(
                CloseableRegistry closeableRegistry, ReadOptions readOptions) throws IOException {

            final List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators =
                    new ArrayList<>(metaData.size());

            int kvStateId = 0;

            for (MetaData metaDataEntry : metaData) {
                RocksIteratorWrapper rocksIteratorWrapper =
                        createRocksIteratorWrapper(
                                db,
                                metaDataEntry.rocksDbKvStateInfo.columnFamilyHandle,
                                metaDataEntry.stateSnapshotTransformer,
                                readOptions);
                kvStateIterators.add(Tuple2.of(rocksIteratorWrapper, kvStateId));
                closeableRegistry.registerCloseable(rocksIteratorWrapper);
                ++kvStateId;
            }

            return kvStateIterators;
        }

        private static RocksIteratorWrapper createRocksIteratorWrapper(
                RocksDB db,
                ColumnFamilyHandle columnFamilyHandle,
                StateSnapshotTransformer<byte[]> stateSnapshotTransformer,
                ReadOptions readOptions) {
            RocksIterator rocksIterator = db.newIterator(columnFamilyHandle, readOptions);
            return stateSnapshotTransformer == null
                    ? new RocksIteratorWrapper(rocksIterator)
                    : new RocksTransformingIteratorWrapper(rocksIterator, stateSnapshotTransformer);
        }

        @Override
        public List<StateMetaInfoSnapshot> getMetaInfoSnapshots() {
            return stateMetaInfoSnapshots;
        }

        @Override
        public void release() {
            db.releaseSnapshot(snapshot);
            IOUtils.closeQuietly(snapshot);
            IOUtils.closeQuietly(lease);
        }

        private static class MetaData {
            final RocksDbKvStateInfo rocksDbKvStateInfo;
            final StateSnapshotTransformer<byte[]> stateSnapshotTransformer;

            private MetaData(
                    RocksDbKvStateInfo rocksDbKvStateInfo,
                    StateSnapshotTransformer<byte[]> stateSnapshotTransformer) {

                this.rocksDbKvStateInfo = rocksDbKvStateInfo;
                this.stateSnapshotTransformer = stateSnapshotTransformer;
            }
        }
    }
}
