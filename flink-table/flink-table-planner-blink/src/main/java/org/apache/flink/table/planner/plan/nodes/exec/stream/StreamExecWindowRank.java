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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.PartitionSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.operators.rank.window.WindowRankOperatorBuilder;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for WindowRank. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamExecWindowRank extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private static final long WINDOW_RANK_MEMORY_RATIO = 100;

    public static final String FIELD_NAME_RANK_TYPE = "rankType";
    public static final String FIELD_NAME_PARTITION_SPEC = "partitionSpec";
    public static final String FIELD_NAME_SORT_SPEC = "sortSpec";
    public static final String FIELD_NAME_RANK_RANG = "rankRange";
    public static final String FIELD_NAME_OUTPUT_RANK_NUMBER = "outputRowNumber";
    public static final String FIELD_NAME_WINDOWING = "windowing";

    @JsonProperty(FIELD_NAME_RANK_TYPE)
    private final RankType rankType;

    @JsonProperty(FIELD_NAME_PARTITION_SPEC)
    private final PartitionSpec partitionSpec;

    @JsonProperty(FIELD_NAME_SORT_SPEC)
    private final SortSpec sortSpec;

    @JsonProperty(FIELD_NAME_RANK_RANG)
    private final RankRange rankRange;

    @JsonProperty(FIELD_NAME_OUTPUT_RANK_NUMBER)
    private final boolean outputRankNumber;

    @JsonProperty(FIELD_NAME_WINDOWING)
    private final WindowingStrategy windowing;

    public StreamExecWindowRank(
            RankType rankType,
            PartitionSpec partitionSpec,
            SortSpec sortSpec,
            RankRange rankRange,
            boolean outputRankNumber,
            WindowingStrategy windowing,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(Collections.singletonList(inputProperty), outputType, description);
        this.rankType = rankType;
        this.rankRange = rankRange;
        this.sortSpec = sortSpec;
        this.partitionSpec = partitionSpec;
        this.outputRankNumber = outputRankNumber;
        this.windowing = windowing;
    }

    @JsonCreator
    public StreamExecWindowRank(
            @JsonProperty(FIELD_NAME_RANK_TYPE) RankType rankType,
            @JsonProperty(FIELD_NAME_PARTITION_SPEC) PartitionSpec partitionSpec,
            @JsonProperty(FIELD_NAME_SORT_SPEC) SortSpec sortSpec,
            @JsonProperty(FIELD_NAME_RANK_RANG) RankRange rankRange,
            @JsonProperty(FIELD_NAME_OUTPUT_RANK_NUMBER) boolean outputRankNumber,
            @JsonProperty(FIELD_NAME_WINDOWING) WindowingStrategy windowing,
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.rankType = checkNotNull(rankType);
        this.partitionSpec = checkNotNull(partitionSpec);
        this.sortSpec = checkNotNull(sortSpec);
        this.rankRange = checkNotNull(rankRange);
        this.outputRankNumber = outputRankNumber;
        this.windowing = checkNotNull(windowing);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        // validate rank type
        switch (rankType) {
            case ROW_NUMBER:
                break;
            case RANK:
                throw new TableException("RANK() on window rank is not supported currently.");
            case DENSE_RANK:
                throw new TableException("DENSE_RANK() on window rank is not supported currently.");
            default:
                throw new TableException(
                        String.format(
                                "Rank function %s on window rank is not supported currently.",
                                rankType));
        }

        // validate rank range
        ConstantRankRange constantRankRange;
        if (rankRange instanceof ConstantRankRange) {
            constantRankRange = (ConstantRankRange) rankRange;
        } else {
            throw new TableException(
                    String.format(
                            "Rank strategy %s on window rank is not supported currently.",
                            rankRange));
        }

        // validate window strategy
        if (!windowing.isRowtime()) {
            throw new TableException("PROC-TIME on window rank is not supported currently.");
        }
        int windowEndIndex;
        if (windowing instanceof WindowAttachedWindowingStrategy) {
            windowEndIndex = ((WindowAttachedWindowingStrategy) windowing).getWindowEnd();
        } else {
            throw new UnsupportedOperationException(
                    windowing.getClass().getName() + " is not supported yet.");
        }

        ExecEdge inputEdge = getInputEdges().get(0);
        Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        RowType inputType = (RowType) inputEdge.getOutputType();
        InternalTypeInfo<RowData> inputRowTypeInfo = InternalTypeInfo.of(inputType);
        int[] sortFields = sortSpec.getFieldIndices();
        RowDataKeySelector sortKeySelector =
                KeySelectorUtil.getRowDataSelector(sortFields, inputRowTypeInfo);

        SortSpec.SortSpecBuilder builder = SortSpec.builder();
        IntStream.range(0, sortFields.length)
                .forEach(
                        idx ->
                                builder.addField(
                                        idx,
                                        sortSpec.getFieldSpec(idx).getIsAscendingOrder(),
                                        sortSpec.getFieldSpec(idx).getNullIsLast()));
        SortSpec sortSpecInSortKey = builder.build();
        TableConfig tableConfig = planner.getTableConfig();
        GeneratedRecordComparator sortKeyComparator =
                ComparatorCodeGenerator.gen(
                        tableConfig,
                        "StreamExecSortComparator",
                        RowType.of(sortSpec.getFieldTypes(inputType)),
                        sortSpecInSortKey);
        RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        partitionSpec.getFieldIndices(), inputRowTypeInfo);

        OneInputStreamOperator<RowData, RowData> operator =
                WindowRankOperatorBuilder.builder()
                        .inputSerializer(new RowDataSerializer(inputType))
                        .keySerializer(
                                (PagedTypeSerializer<RowData>)
                                        selector.getProducedType().toSerializer())
                        .sortKeySelector(sortKeySelector)
                        .sortKeyComparator(sortKeyComparator)
                        .outputRankNumber(outputRankNumber)
                        .rankStart(constantRankRange.getRankStart())
                        .rankEnd(constantRankRange.getRankEnd())
                        .windowEndIndex(windowEndIndex)
                        .build();

        OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        getDescription(),
                        SimpleOperatorFactory.of(operator),
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism(),
                        WINDOW_RANK_MEMORY_RATIO);

        // set KeyType and Selector for state
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());
        return transform;
    }
}
