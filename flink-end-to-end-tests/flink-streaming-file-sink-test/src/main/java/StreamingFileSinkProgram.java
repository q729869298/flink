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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.Bucketer;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketers.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test program for the {@link StreamingFileSink}.
 *
 * <p>Uses a source that steadily emits a deterministic set of records over 60 seconds,
 * after which it idles and waits for job cancellation. Every record has a unique index that is
 * written to the file.
 *
 * <p>The sink rolls on each checkpoint, with each part file containing a sequence of integers.
 * Adding all committed part files together, and numerically sorting the contents, should
 * result in a complete sequence from 0 (inclusive) to 60000 (exclusive).
 */
public enum StreamingFileSinkProgram {
	;

	public static void main(final String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		final String outputPath = params.getRequired("outputPath");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(4);
		env.enableCheckpointing(5000L);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10L, TimeUnit.SECONDS)));

		final StreamingFileSink<Tuple2<Integer, Integer>> sink = StreamingFileSink
			.forRowFormat(new Path(outputPath), (Encoder<Tuple2<Integer, Integer>>) (element, stream) -> {
				PrintStream out = new PrintStream(stream);
				out.println(element.f1);
			})
			.withBucketer(new KeyBucketer())
			.withRollingPolicy(new OnCheckpointRollingPolicy<>())
			.build();

		// generate data, shuffle, sink
		env.addSource(new Generator(10, 10, 60))
			.keyBy(0)
			.addSink(sink);

		env.execute("StreamingFileSinkProgram");
	}


	/**
	 * Use first field for buckets.
	 */
	public static final class KeyBucketer implements Bucketer<Tuple2<Integer, Integer>, String> {

		private static final long serialVersionUID = 987325769970523326L;

		@Override
		public String getBucketId(final Tuple2<Integer, Integer> element, final Context context) {
			return String.valueOf(element.f0);
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return SimpleVersionedStringSerializer.INSTANCE;
		}
	}

	/**
	 * Data-generating source function.
	 */
	public static final class Generator implements SourceFunction<Tuple2<Integer, Integer>>, ListCheckpointed<Tuple2<Integer, Long>> {

		private static final long serialVersionUID = -2819385275681175792L;

		private final int numKeys;
		private final int idlenessMs;
		private final int durationMs;

		private long ms = 0L;

		private volatile int numRecordsEmitted = 0;
		private volatile boolean canceled = false;

		Generator(final int numKeys, final int idlenessMs, final int durationSeconds) {
			this.numKeys = numKeys;
			this.idlenessMs = idlenessMs;
			this.durationMs = durationSeconds * 1000;
		}

		@Override
		public void run(final SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
			while (ms < durationMs) {
				synchronized (ctx.getCheckpointLock()) {
					for (int i = 0; i < numKeys; i++) {
						ctx.collect(Tuple2.of(i, numRecordsEmitted));
						numRecordsEmitted++;
					}
					ms += idlenessMs;
				}
				Thread.sleep(idlenessMs);
			}

			while (!canceled) {
				Thread.sleep(50);
			}

		}

		@Override
		public void cancel() {
			canceled = true;
		}

		@Override
		public List<Tuple2<Integer, Long>> snapshotState(final long checkpointId, final long timestamp) {
			return Collections.singletonList(Tuple2.of(numRecordsEmitted, ms));
		}

		@Override
		public void restoreState(final List<Tuple2<Integer, Long>> states) {
			for (final Tuple2<Integer, Long> state : states) {
				numRecordsEmitted += state.f0;
				ms += state.f1;
			}
		}
	}
}
