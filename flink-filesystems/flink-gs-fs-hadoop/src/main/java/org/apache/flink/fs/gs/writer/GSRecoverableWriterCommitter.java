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

package org.apache.flink.fs.gs.writer;

import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.gs.GSFileSystemOptions;
import org.apache.flink.fs.gs.storage.GSBlobIdentifier;
import org.apache.flink.fs.gs.storage.GSBlobStorage;
import org.apache.flink.fs.gs.utils.BlobUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** The committer for the GS recoverable writer. */
class GSRecoverableWriterCommitter implements RecoverableFsDataOutputStream.Committer {

    /** The underlying blob storage. */
    private final GSBlobStorage storage;

    /** The GS file system options. */
    private final GSFileSystemOptions options;

    /** The recoverable writer instance. */
    private final GSRecoverableWriter writer;

    /** The recoverable writer state for the commit operation. */
    private final GSRecoverableWriterState state;

    GSRecoverableWriterCommitter(
            GSBlobStorage storage,
            GSFileSystemOptions options,
            GSRecoverableWriter writer,
            GSRecoverableWriterState state) {
        this.storage = Preconditions.checkNotNull(storage);
        this.options = Preconditions.checkNotNull(options);
        this.writer = Preconditions.checkNotNull(writer);
        this.state = Preconditions.checkNotNull(state);
    }

    @Override
    public void commit() throws IOException {

        // compose all the component blob ids into the final blob id. if the component blob ids are
        // in the same bucket as the final blob id, this can be done directly. otherwise, we must
        // compose to a new temporary blob id in the same bucket as the component blob ids and
        // then copy that blob to the final blob location
        if (state.finalBlobIdentifier.bucketName.equals(state.getTemporaryBucketName(options))) {

            // compose directly to final blob
            composeBlobs(state.getComponentBlobIds(options), state.finalBlobIdentifier);

        } else {

            // compose to a temporary blob id, then copy to final blob id
            GSBlobIdentifier intermediateBlobIdentifier = state.createTemporaryBlobId(options);
            composeBlobs(state.getComponentBlobIds(options), intermediateBlobIdentifier);
            storage.copy(intermediateBlobIdentifier, state.finalBlobIdentifier);
        }

        // clean up after commit
        writer.cleanupRecoverableState(state);
    }

    @Override
    public void commitAfterRecovery() throws IOException {

        // is the final blob present? if so, we have completed the compose step already
        Optional<GSBlobStorage.BlobMetadata> blobMetadata =
                storage.getMetadata(state.finalBlobIdentifier);
        if (blobMetadata.isPresent()) {

            // the final blob is present, so assume the compose succeeded.
            // clean up in case any temporary files are still around
            writer.cleanupRecoverableState(state);

        } else {

            // the final blob isn't present, so do a full commit
            commit();
        }
    }

    @Override
    public RecoverableWriter.CommitRecoverable getRecoverable() {
        return state;
    }

    /**
     * Helper to compose an arbitrary number of blobs into a final blob, staying under the
     * COMPOSE_MAX_BLOBS limit for any individual compose operation.
     *
     * @param sourceBlobIdentifiers The source blob ids to compose
     * @param targetBlobIdentifier The target blob id for the composed result
     */
    private void composeBlobs(
            List<GSBlobIdentifier> sourceBlobIdentifiers, GSBlobIdentifier targetBlobIdentifier) {
        Preconditions.checkNotNull(sourceBlobIdentifiers);
        Preconditions.checkArgument(sourceBlobIdentifiers.size() > 0);
        Preconditions.checkNotNull(targetBlobIdentifier);

        // split the source list into two parts; first, the ones we can compose in this operation
        // (up to COMPOSE_MAX_BLOBS), and, second, whichever blobs are left over
        final int composeToIndex =
                Math.min(BlobUtils.COMPOSE_MAX_BLOBS, sourceBlobIdentifiers.size());
        List<GSBlobIdentifier> composeBlobIds = sourceBlobIdentifiers.subList(0, composeToIndex);
        List<GSBlobIdentifier> remainingBlobIds =
                sourceBlobIdentifiers.subList(composeToIndex, sourceBlobIdentifiers.size());

        // determine the resulting blob id for this compose operation. if this is the last compose,
        // i.e. if there are no remaining blob ids, then the composed blob id is the originally
        // specified target blob id. otherwise, we must create an intermediate blob id to hold the
        // result of this compose operation
        GSBlobIdentifier composedBlobId =
                remainingBlobIds.isEmpty()
                        ? targetBlobIdentifier
                        : state.createTemporaryBlobId(options);

        // compose the blobs
        storage.compose(composeBlobIds, composedBlobId);

        // if we have remaining blobs, add the composed blob id to the beginning of the list
        // of remaining blob ids, and recurse
        if (!remainingBlobIds.isEmpty()) {
            remainingBlobIds.add(0, composedBlobId);
            composeBlobs(remainingBlobIds, targetBlobIdentifier);
        }
    }
}
