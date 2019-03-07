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
package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.plan.util.AggregateUtil

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.tools.RelBuilder

import java.util

/**
  * Batch physical RelNode for local hash-based aggregate operator.
  *
  * @see [[BatchExecGroupAggregateBase]] for more info.
  */
class BatchExecLocalHashAggregate(
    cluster: RelOptCluster,
    relBuilder: RelBuilder,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
    rowRelDataType: RelDataType,
    inputRelDataType: RelDataType,
    grouping: Array[Int],
    auxGrouping: Array[Int])
  extends BatchExecHashAggregateBase(
    cluster,
    relBuilder,
    traitSet,
    inputRel,
    aggCallToAggFunction,
    rowRelDataType,
    inputRelDataType,
    grouping,
    auxGrouping,
    isMerge = false,
    isFinal = false) {

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy",
        AggregateUtil.groupingToString(inputRelDataType, grouping), grouping.nonEmpty)
      .itemIf("auxGrouping",
        AggregateUtil.groupingToString(inputRelDataType, auxGrouping), auxGrouping.nonEmpty)
      .item("select", AggregateUtil.aggregationToString(
        inputRelDataType,
        grouping,
        auxGrouping,
        rowRelDataType,
        aggCallToAggFunction.map(_._1),
        aggCallToAggFunction.map(_._2),
        isMerge = false,
        isGlobal = false))
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecLocalHashAggregate(
      cluster,
      relBuilder,
      traitSet,
      inputs.get(0),
      aggCallToAggFunction,
      getRowType,
      inputRelDataType,
      grouping,
      auxGrouping)
  }

}
