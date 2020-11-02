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

package org.apache.flink.connector.file.sink.writer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Writer implementation for {@link FileSink}.
 */
public class FileWriter<IN, BucketID>
		implements SinkWriter<IN, FileSinkCommittable, FileWriterBucketState<BucketID>> {

	private static final Logger LOG = LoggerFactory.getLogger(FileWriter.class);

	// ------------------------ configuration fields --------------------------

	private final Path basePath;

	private final FileWriterBucketFactory<IN, BucketID> bucketFactory;

	private final BucketAssigner<IN, BucketID> bucketAssigner;

	private final BucketWriter<IN, BucketID> bucketWriter;

	private final RollingPolicy<IN, BucketID> rollingPolicy;

	// --------------------------- runtime fields -----------------------------

	private final BucketerContext bucketerContext;

	private final Map<BucketID, FileWriterBucket<IN, BucketID>> activeBuckets;

	private final OutputFileConfig outputFileConfig;

	// --------------------------- State Related Fields -----------------------------

	private final FileWriterBucketStateSerializer<BucketID> bucketStateSerializer;

	/**
	 * A constructor creating a new empty bucket manager.
	 *
	 * @param basePath The base path for our buckets.
	 * @param bucketAssigner The {@link BucketAssigner} provided by the user.
	 * @param bucketFactory The {@link FileWriterBucketFactory} to be used to create buckets.
	 * @param bucketWriter The {@link BucketWriter} to be used when writing data.
	 * @param rollingPolicy The {@link RollingPolicy} as specified by the user.
	 */
	public FileWriter(
			final Path basePath,
			final BucketAssigner<IN, BucketID> bucketAssigner,
			final FileWriterBucketFactory<IN, BucketID> bucketFactory,
			final BucketWriter<IN, BucketID> bucketWriter,
			final RollingPolicy<IN, BucketID> rollingPolicy,
			final OutputFileConfig outputFileConfig) {

		this.basePath = checkNotNull(basePath);
		this.bucketAssigner = checkNotNull(bucketAssigner);
		this.bucketFactory = checkNotNull(bucketFactory);
		this.bucketWriter = checkNotNull(bucketWriter);
		this.rollingPolicy = checkNotNull(rollingPolicy);

		this.outputFileConfig = checkNotNull(outputFileConfig);

		this.activeBuckets = new HashMap<>();
		this.bucketerContext = new BucketerContext();

		this.bucketStateSerializer = new FileWriterBucketStateSerializer<>(
				bucketWriter.getProperties().getInProgressFileRecoverableSerializer(),
				bucketAssigner.getSerializer());
	}

	/**
	 * Initializes the state after recovery from a failure.
	 *
	 * <p>During this process:
	 * <ol>
	 *     <li>we set the initial value for part counter to the maximum value used before across all tasks and buckets.
	 *     This guarantees that we do not overwrite valid data,</li>
	 *     <li>we commit any pending files for previous checkpoints (previous to the last successful one from which we restore),</li>
	 *     <li>we resume writing to the previous in-progress file of each bucket, and</li>
	 *     <li>if we receive multiple states for the same bucket, we merge them.</li>
	 * </ol>
	 *
	 * @param bucketStates the state holding recovered state about active buckets.
	 *
	 * @throws IOException if anything goes wrong during retrieving the state or restoring/committing of any
	 * 		in-progress/pending part files
	 */
	public void initializeState(List<FileWriterBucketState<BucketID>> bucketStates) throws IOException {
		checkNotNull(bucketStates, "The retrieved state was null.");

		for (FileWriterBucketState<BucketID> state : bucketStates) {
			BucketID bucketId = state.getBucketId();

			if (LOG.isDebugEnabled()) {
				LOG.debug("Restoring: {}", state);
			}

			FileWriterBucket<IN, BucketID> restoredBucket = bucketFactory.restoreBucket(
					bucketWriter,
					rollingPolicy,
					state,
					outputFileConfig);

			updateActiveBucketId(bucketId, restoredBucket);
		}
	}

	private void updateActiveBucketId(
			BucketID bucketId,
			FileWriterBucket<IN, BucketID> restoredBucket) throws IOException {
		final FileWriterBucket<IN, BucketID> bucket = activeBuckets.get(bucketId);
		if (bucket != null) {
			bucket.merge(restoredBucket);
		} else {
			activeBuckets.put(bucketId, restoredBucket);
		}
	}

	@Override
	public void write(IN element, Context context) throws IOException {
		// setting the values in the bucketer context
		bucketerContext.update(
				context.timestamp(),
				context.currentWatermark());

		final BucketID bucketId = bucketAssigner.getBucketId(element, bucketerContext);
		final FileWriterBucket<IN, BucketID> bucket = getOrCreateBucketForBucketId(bucketId);
		bucket.write(element);
	}

	@Override
	public List<FileSinkCommittable> prepareCommit(boolean flush) throws IOException {
		List<FileSinkCommittable> committables = new ArrayList<>();

		// Every time before we prepare commit, we first check and remove the inactive
		// buckets. Checking the activeness right before pre-committing avoid re-creating
		// the bucket every time if the bucket use OnCheckpointingRollingPolicy.
		Iterator<Map.Entry<BucketID, FileWriterBucket<IN, BucketID>>> activeBucketIt =
				activeBuckets.entrySet().iterator();
		while (activeBucketIt.hasNext()) {
			Map.Entry<BucketID, FileWriterBucket<IN, BucketID>> entry = activeBucketIt.next();
			if (!entry.getValue().isActive()) {
				activeBucketIt.remove();
			} else {
				committables.addAll(entry.getValue().prepareCommit(flush));
			}
		}

		return committables;
	}

	@Override
	public List<FileWriterBucketState<BucketID>> snapshotState() throws IOException {
		checkState(
				bucketWriter != null && bucketStateSerializer != null,
				"sink has not been initialized");

		List<FileWriterBucketState<BucketID>> state = new ArrayList<>();
		for (FileWriterBucket<IN, BucketID> bucket : activeBuckets.values()) {
			state.add(bucket.snapshotState());
		}

		return state;
	}

	private FileWriterBucket<IN, BucketID> getOrCreateBucketForBucketId(BucketID bucketId) throws IOException {
		FileWriterBucket<IN, BucketID> bucket = activeBuckets.get(bucketId);
		if (bucket == null) {
			final Path bucketPath = assembleBucketPath(bucketId);
			bucket = bucketFactory.getNewBucket(
					bucketId,
					bucketPath,
					bucketWriter,
					rollingPolicy,
					outputFileConfig);
			activeBuckets.put(bucketId, bucket);
		}
		return bucket;
	}

	@Override
	public void close() {
		if (activeBuckets != null) {
			activeBuckets.values().forEach(FileWriterBucket::disposePartFile);
		}
	}

	private Path assembleBucketPath(BucketID bucketId) {
		final String child = bucketId.toString();
		if ("".equals(child)) {
			return basePath;
		}
		return new Path(basePath, child);
	}

	/**
	 * The {@link BucketAssigner.Context} exposed to the
	 * {@link BucketAssigner#getBucketId(Object, BucketAssigner.Context)}
	 * whenever a new incoming element arrives.
	 */
	private static final class BucketerContext implements BucketAssigner.Context {

		@Nullable
		private Long elementTimestamp;

		private long currentWatermark;

		private BucketerContext() {
			this.elementTimestamp = null;
			this.currentWatermark = Long.MIN_VALUE;
		}

		void update(@Nullable Long elementTimestamp, long watermark) {
			this.elementTimestamp = elementTimestamp;
			this.currentWatermark = watermark;
		}

		@Override
		public long currentProcessingTime() {
			throw new UnsupportedOperationException("not supported");
		}

		@Override
		public long currentWatermark() {
			return currentWatermark;
		}

		@Override
		@Nullable
		public Long timestamp() {
			return elementTimestamp;
		}
	}

	// --------------------------- Testing Methods -----------------------------

	@VisibleForTesting
	Map<BucketID, FileWriterBucket<IN, BucketID>> getActiveBuckets() {
		return activeBuckets;
	}
}
