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

package org.apache.flink.streaming.api.functions.source.datagen;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.util.Preconditions;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A stateful, re-scalable {@link DataGenerator} that emits each number from a given interval
 * exactly once, possibly in parallel.
 */
@Experimental
public abstract class SequenceGenerator<T> implements DataGenerator<T> {

    private final long start;
    private final long end;

    private transient ListState<Long> checkpointedState;
    protected transient Deque<Long> valuesToEmit;

    /**
     * Creates a DataGenerator that emits all numbers from the given interval exactly once.
     *
     * <p>The {@code SequenceGenerator} requires that the {@code inclEnd} must be greater than the
     * {@code inclStart} and that the total number cannot be greater than {@code Long.MAX_VALUE -
     * 1}.
     *
     * @param inclStart Start of the range of numbers to emit.
     * @param inclEnd End of the range of numbers to emit.
     */
    protected SequenceGenerator(long inclStart, long inclEnd) {
        Preconditions.checkArgument(
                inclEnd > inclStart,
                "The start value (%s) cannot be greater than the end value (%s).",
                inclStart,
                inclEnd);
        Preconditions.checkArgument(
                inclEnd - inclStart <= Long.MAX_VALUE - 1,
                "The total size of range (%s, %s) exceeds the maximum limit: Long.MAX_VALUE - 1.",
                inclStart,
                inclEnd);
        this.start = inclStart;
        this.end = inclEnd;
    }

    @Override
    public void open(
            String name, FunctionInitializationContext context, RuntimeContext runtimeContext)
            throws Exception {
        Preconditions.checkState(
                this.checkpointedState == null,
                "The " + getClass().getSimpleName() + " has already been initialized.");

        this.checkpointedState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        name + "-sequence-state", LongSerializer.INSTANCE));
        this.valuesToEmit = new ArrayDeque<>();
        if (context.isRestored()) {
            // upon restoring

            for (Long v : this.checkpointedState.get()) {
                this.valuesToEmit.add(v);
            }
        } else {
            // the first time the job is executed
            final int stepSize = runtimeContext.getNumberOfParallelSubtasks();
            final int taskIdx = runtimeContext.getIndexOfThisSubtask();
            final long congruence = start + taskIdx;

            long totalNoOfElements = Math.abs(end - start + 1);
            final long baseSize = totalNoOfElements / stepSize;

            final long toCollect =
                    (totalNoOfElements % stepSize > taskIdx) ? baseSize + 1 : baseSize;

            for (long collected = 0; collected < toCollect; collected++) {
                this.valuesToEmit.add(collected * stepSize + congruence);
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        Preconditions.checkState(
                this.checkpointedState != null,
                "The " + getClass().getSimpleName() + " state has not been properly initialized.");

        this.checkpointedState.clear();
        for (Long v : this.valuesToEmit) {
            this.checkpointedState.add(v);
        }
    }

    @Override
    public boolean hasNext() {
        return !this.valuesToEmit.isEmpty();
    }

    @VisibleForTesting
    public long getStart() {
        return start;
    }

    @VisibleForTesting
    public long getEnd() {
        return end;
    }

    public static SequenceGenerator<Long> longGenerator(long start, long end) {
        return new SequenceGenerator<Long>(start, end) {
            @Override
            public Long next() {
                return valuesToEmit.poll();
            }
        };
    }

    public static SequenceGenerator<Integer> intGenerator(int start, int end) {
        return new SequenceGenerator<Integer>(start, end) {
            @Override
            public Integer next() {
                return valuesToEmit.poll().intValue();
            }
        };
    }

    public static SequenceGenerator<Short> shortGenerator(short start, short end) {
        return new SequenceGenerator<Short>(start, end) {
            @Override
            public Short next() {
                return valuesToEmit.poll().shortValue();
            }
        };
    }

    public static SequenceGenerator<Byte> byteGenerator(byte start, byte end) {
        return new SequenceGenerator<Byte>(start, end) {
            @Override
            public Byte next() {
                return valuesToEmit.poll().byteValue();
            }
        };
    }

    public static SequenceGenerator<Float> floatGenerator(short start, short end) {
        return new SequenceGenerator<Float>(start, end) {
            @Override
            public Float next() {
                return valuesToEmit.poll().floatValue();
            }
        };
    }

    public static SequenceGenerator<Double> doubleGenerator(int start, int end) {
        return new SequenceGenerator<Double>(start, end) {
            @Override
            public Double next() {
                return valuesToEmit.poll().doubleValue();
            }
        };
    }

    public static SequenceGenerator<BigDecimal> bigDecimalGenerator(
            int start, int end, int precision, int scale) {
        return new SequenceGenerator<BigDecimal>(start, end) {
            @Override
            public BigDecimal next() {
                BigDecimal decimal =
                        new BigDecimal(
                                valuesToEmit.poll().doubleValue(), new MathContext(precision));
                return decimal.setScale(scale, RoundingMode.DOWN);
            }
        };
    }

    public static SequenceGenerator<String> stringGenerator(long start, long end) {
        return new SequenceGenerator<String>(start, end) {
            @Override
            public String next() {
                return valuesToEmit.poll().toString();
            }
        };
    }
}
