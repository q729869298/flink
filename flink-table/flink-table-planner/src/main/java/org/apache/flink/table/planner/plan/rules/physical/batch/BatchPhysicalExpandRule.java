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

package org.apache.flink.table.planner.plan.rules.physical.batch;

import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalExpand;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalExpand;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/** Rule that converts {@link FlinkLogicalExpand} to @{@link BatchPhysicalExpand}. */
public class BatchPhysicalExpandRule extends ConverterRule {

    public static final RelOptRule INSTANCE =
            new BatchPhysicalExpandRule(
                    Config.INSTANCE
                            .withConversion(
                                    FlinkLogicalExpand.class,
                                    FlinkConventions.LOGICAL(),
                                    FlinkConventions.BATCH_PHYSICAL(),
                                    "BatchPhysicalExpandRule")
                            .withRuleFactory(BatchPhysicalExpandRule::new));

    protected BatchPhysicalExpandRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        FlinkLogicalExpand expand = (FlinkLogicalExpand) rel;
        RelTraitSet newTrait = rel.getTraitSet().replace(FlinkConventions.BATCH_PHYSICAL());
        RelNode newInput = RelOptRule.convert(expand.getInput(), FlinkConventions.BATCH_PHYSICAL());
        return new BatchPhysicalExpand(
                rel.getCluster(), newTrait, newInput, expand.projects(), expand.expandIdIndex());
    }
}
