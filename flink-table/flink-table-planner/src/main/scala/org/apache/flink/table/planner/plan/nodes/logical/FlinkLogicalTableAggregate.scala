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
package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.calcite.{LogicalTableAggregate, TableAggregate}

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import java.util
import java.util.{List => JList}

/**
 * Sub-class of [[TableAggregate]] that is a relational expression which performs aggregations but
 * outputs 0 or more records for a group.
 */
class FlinkLogicalTableAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    groupSet: ImmutableBitSet,
    groupSets: util.List[ImmutableBitSet],
    aggCalls: util.List[AggregateCall])
  extends TableAggregate(cluster, traitSet, input, groupSet, groupSets, aggCalls)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, inputs: JList[RelNode]): RelNode = {
    new FlinkLogicalTableAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      groupSet,
      groupSets,
      aggCalls
    )
  }
}

private class FlinkLogicalTableAggregateConverter(config: Config) extends ConverterRule(config) {

  override def convert(rel: RelNode): RelNode = {
    val agg = rel.asInstanceOf[LogicalTableAggregate]
    val traitSet = rel.getTraitSet.replace(FlinkConventions.LOGICAL)
    val newInput = RelOptRule.convert(agg.getInput, FlinkConventions.LOGICAL)

    new FlinkLogicalTableAggregate(
      rel.getCluster,
      traitSet,
      newInput,
      agg.getGroupSet,
      agg.getGroupSets,
      agg.aggCalls)
  }
}

object FlinkLogicalTableAggregate {
  val CONVERTER: ConverterRule = new FlinkLogicalTableAggregateConverter(
    Config.INSTANCE.withConversion(
      classOf[LogicalTableAggregate],
      Convention.NONE,
      FlinkConventions.LOGICAL,
      "FlinkLogicalTableAggregateConverter"))
}
