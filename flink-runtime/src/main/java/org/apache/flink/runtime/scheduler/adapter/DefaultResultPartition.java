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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.util.IterableUtils;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link SchedulingResultPartition}. */
class DefaultResultPartition implements SchedulingResultPartition {

    private final IntermediateResultPartitionID resultPartitionId;

    private final IntermediateDataSetID intermediateDataSetId;

    private final ResultPartitionType partitionType;

    private final Supplier<ResultPartitionState> resultPartitionStateSupplier;

    private DefaultExecutionVertex producer;

    private final ConsumerVertexGroup consumerVertexGroup;

    private final Function<ExecutionVertexID, DefaultExecutionVertex> executionVertexRetriever;

    private final Supplier<List<ConsumedPartitionGroup>> consumerPartitionGroupSupplier;

    DefaultResultPartition(
            IntermediateResultPartitionID partitionId,
            IntermediateDataSetID intermediateDataSetId,
            ResultPartitionType partitionType,
            Supplier<ResultPartitionState> resultPartitionStateSupplier,
            ConsumerVertexGroup consumerVertexGroup,
            Function<ExecutionVertexID, DefaultExecutionVertex> executionVertexRetriever,
            Supplier<List<ConsumedPartitionGroup>> consumerPartitionGroupSupplier) {
        this.resultPartitionId = checkNotNull(partitionId);
        this.intermediateDataSetId = checkNotNull(intermediateDataSetId);
        this.partitionType = checkNotNull(partitionType);
        this.resultPartitionStateSupplier = checkNotNull(resultPartitionStateSupplier);
        this.consumerVertexGroup = consumerVertexGroup;
        this.executionVertexRetriever = executionVertexRetriever;
        this.consumerPartitionGroupSupplier = consumerPartitionGroupSupplier;
    }

    @VisibleForTesting
    DefaultResultPartition(
            IntermediateResultPartitionID partitionId,
            IntermediateDataSetID intermediateDataSetId,
            ResultPartitionType partitionType,
            Supplier<ResultPartitionState> resultPartitionStateSupplier) {
        this(
                partitionId,
                intermediateDataSetId,
                partitionType,
                resultPartitionStateSupplier,
                null,
                null,
                null);
    }

    @Override
    public IntermediateResultPartitionID getId() {
        return resultPartitionId;
    }

    @Override
    public IntermediateDataSetID getResultId() {
        return intermediateDataSetId;
    }

    @Override
    public ResultPartitionType getResultType() {
        return partitionType;
    }

    @Override
    public ResultPartitionState getState() {
        return resultPartitionStateSupplier.get();
    }

    @Override
    public DefaultExecutionVertex getProducer() {
        return producer;
    }

    @Override
    public Iterable<DefaultExecutionVertex> getConsumers() {
        return IterableUtils.toStream(consumerVertexGroup)
                .map(executionVertexRetriever)
                .collect(Collectors.toList());
    }

    @Override
    public ConsumerVertexGroup getConsumerVertexGroup() {
        return consumerVertexGroup;
    }

    @Override
    public List<ConsumedPartitionGroup> getConsumedPartitionGroups() {
        return consumerPartitionGroupSupplier.get();
    }

    void setProducer(DefaultExecutionVertex vertex) {
        producer = checkNotNull(vertex);
    }
}
