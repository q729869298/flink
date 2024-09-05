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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * A factory that creates {@link BulkPartWriter BulkPartWriters}.
 *
 * @param <IN> The type of input elements.
 * @param <BucketID> The type of ids for the buckets, as returned by the {@link BucketAssigner}.
 */
@Internal
public class BulkBucketWriter<IN, BucketID>
        extends OutputStreamBasedPartFileWriter.OutputStreamBasedBucketWriter<IN, BucketID> {

    private final BulkWriter.Factory<IN> writerFactory;

    public BulkBucketWriter(
            final RecoverableWriter recoverableWriter, final BulkWriter.Factory<IN> writerFactory) {
        super(recoverableWriter);
        this.writerFactory = writerFactory;
    }

    @Override
    public InProgressFileWriter<IN, BucketID> resumeFrom(
            final BucketID bucketId,
            final RecoverableFsDataOutputStream stream,
            final Path path,
            final RecoverableWriter.ResumeRecoverable resumable,
            final long creationTime)
            throws IOException {

        Preconditions.checkNotNull(stream);
        Preconditions.checkNotNull(resumable);

        final BulkWriter<IN> writer = writerFactory.create(stream);
        return new BulkPartWriter<>(bucketId, path, stream, writer, creationTime);
    }

    @Override
    public InProgressFileWriter<IN, BucketID> openNew(
            final BucketID bucketId,
            final RecoverableFsDataOutputStream stream,
            final Path path,
            final long creationTime,
            final boolean forceNoCompress)
            throws IOException {

        Preconditions.checkNotNull(stream);
        Preconditions.checkNotNull(path);

        final BulkWriter<IN> writer;
        if (forceNoCompress && isCompressWriterFactory()) {
            writer = createWithNoCompression(stream);
        } else {
            writer = writerFactory.create(stream);
        }
        return new BulkPartWriter<>(bucketId, path, stream, writer, creationTime);
    }

    /*
     * Usage of reflection required for the next methods, because the module "flink-compress"
     * cannot be included here, as it causes a circular dependency reference.
     */

    private boolean isCompressWriterFactory() {
        return "CompressWriterFactory".equals(writerFactory.getClass().getSimpleName());
    }

    @SuppressWarnings("unchecked")
    private BulkWriter<IN> createWithNoCompression(final RecoverableFsDataOutputStream stream) {
        try {
            Method method =
                    writerFactory
                            .getClass()
                            .getMethod("createWithNoCompression", FSDataOutputStream.class);
            return (BulkWriter<IN>) method.invoke(writerFactory, stream);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Failed to initialize BulkBucketWriter via"
                            + " CompressWriterFactory#createWithNoCompression(...)",
                    e);
        }
    }
}
