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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.fusion.generator.SourceOpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.fusion.spec.InputAdapterFusionCodegenSpec;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;

/** Batch {@link ExecNode} for multiple operator fusion codegen input, it is adapter source node. */
@ExecNodeMetadata(
        name = "batch-exec-input-adapter",
        version = 1,
        minPlanVersion = FlinkVersion.v1_20,
        minStateVersion = FlinkVersion.v1_20)
public class BatchExecInputAdapter extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private final int multipleInputId;

    public static final String FIELD_NAME_MULTIPLE_INPUT_ID = "multipleInputId";

    public BatchExecInputAdapter(
            int multipleInputId,
            ReadableConfig tableConfig,
            InputProperty inputProperty,
            LogicalType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecInputAdapter.class),
                ExecNodeContext.newPersistedConfig(BatchExecInputAdapter.class, tableConfig),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.multipleInputId = multipleInputId;
    }

    @JsonCreator
    public BatchExecInputAdapter(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_MULTIPLE_INPUT_ID) int multipleInputId,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTY) InputProperty inputProperty,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) LogicalType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(
                id,
                context,
                persistedConfig,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.multipleInputId = multipleInputId;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        return (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
    }

    @Override
    public boolean supportFusionCodegen() {
        return true;
    }

    @Override
    protected OpFusionCodegenSpecGenerator translateToFusionCodegenSpecInternal(
            PlannerBase planner, ExecNodeConfig config, CodeGeneratorContext parentCtx) {
        return new SourceOpFusionCodegenSpecGenerator(
                (RowType) getOutputType(),
                new InputAdapterFusionCodegenSpec(
                        new CodeGeneratorContext(
                                config, planner.getFlinkContext().getClassLoader(), parentCtx),
                        multipleInputId));
    }
}
