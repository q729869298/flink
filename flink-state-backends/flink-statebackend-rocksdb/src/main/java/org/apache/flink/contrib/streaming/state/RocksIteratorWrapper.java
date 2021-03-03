/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksIteratorInterface;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Closeable;

/**
 * This is a wrapper around {@link RocksIterator} to check the iterator status for all the methods
 * mentioned to require this check in the wiki documentation: seek, next, seekToFirst, seekToLast,
 * seekForPrev, and prev. This is required because the iterator may pass the blocks or files it had
 * difficulties in reading (because of IO error, data corruption or other issues) and continue with
 * the next available keys. The status flag may not be OK, even if the iterator is valid. More
 * information can be found <a
 * href="https://github.com/facebook/rocksdb/wiki/Iterator#error-handling">here</a>.
 */
public class RocksIteratorWrapper implements RocksIteratorInterface, Closeable {

    private final RocksIterator iterator;

    private final RocksDBAccessMetric accessMetric;

    private final ColumnFamilyHandleWrapper columnFamilyHandleWrapper;

    private final boolean trackLatencyEnabled;

    public RocksIteratorWrapper(
            @Nonnull RocksIterator iterator,
            @Nullable RocksDBAccessMetric accessMetric,
            @Nullable ColumnFamilyHandleWrapper columnFamilyHandleWrapper) {
        this.iterator = iterator;
        this.accessMetric = accessMetric;
        this.trackLatencyEnabled = accessMetric != null;
        this.columnFamilyHandleWrapper = columnFamilyHandleWrapper;
        Preconditions.checkArgument(
                !trackLatencyEnabled || columnFamilyHandleWrapper != null,
                "Should provide column family handle wrapper when enable latency track.");
    }

    @Override
    public boolean isValid() {
        return this.iterator.isValid();
    }

    @Override
    public void seekToFirst() {
        iterator.seekToFirst();
        status();
    }

    @Override
    public void seekToLast() {
        iterator.seekToFirst();
        status();
    }

    @Override
    public void seek(byte[] target) {
        if (trackLatencyEnabled) {
            seekAndUpdateMetric(target);
        } else {
            originalSeek(target);
        }
    }

    private void seekAndUpdateMetric(final byte[] target) {
        if (accessMetric.checkAndUpdateSeekCounter(columnFamilyHandleWrapper.getColumnFamilyId())) {
            long start = System.nanoTime();
            originalSeek(target);
            long end = System.nanoTime();
            accessMetric.updateHistogram(
                    columnFamilyHandleWrapper.getColumnFamilyId(),
                    RocksDBAccessMetric.SEEK_LATENCY,
                    end - start);
        } else {
            originalSeek(target);
        }
    }

    private void originalSeek(byte[] target) {
        iterator.seek(target);
        status();
    }

    @Override
    public void seekForPrev(byte[] target) {
        iterator.seekForPrev(target);
        status();
    }

    @Override
    public void next() {
        if (trackLatencyEnabled) {
            nextAndUpdateMetric();
        } else {
            originalNext();
        }
    }

    private void nextAndUpdateMetric() {
        if (accessMetric.checkAndUpdateNextCounter(columnFamilyHandleWrapper.getColumnFamilyId())) {
            long start = System.nanoTime();
            originalNext();
            long end = System.nanoTime();
            accessMetric.updateHistogram(
                    columnFamilyHandleWrapper.getColumnFamilyId(),
                    RocksDBAccessMetric.NEXT_LATENCY,
                    end - start);
        } else {
            originalNext();
        }
    }

    private void originalNext() {
        iterator.next();
        status();
    }

    @Override
    public void prev() {
        iterator.prev();
        status();
    }

    @Override
    public void status() {
        try {
            iterator.status();
        } catch (RocksDBException ex) {
            throw new FlinkRuntimeException("Internal exception found in RocksDB", ex);
        }
    }

    public byte[] key() {
        return iterator.key();
    }

    public byte[] value() {
        return iterator.value();
    }

    @Override
    public void close() {
        iterator.close();
    }
}
