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

package org.apache.flink.table.plan.rules.physical.stream

import org.apache.flink.table.api.{AggPhaseEnforcer, PlannerConfigOptions, TableConfigOptions}
import org.apache.flink.table.calcite.{FlinkContext, FlinkTypeFactory}
import org.apache.flink.table.plan.`trait`.{AccMode, AccModeTrait, FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.physical.stream._
import org.apache.flink.table.plan.rules.physical.FlinkExpandConversionRule._
import org.apache.flink.table.plan.util.{AggregateInfoList, AggregateUtil}

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode

import java.util


// use the two stage agg optimize the plan
class TwoStageOptimizedAggregateRule extends RelOptRule(
  operand(classOf[StreamExecGroupAggregate],
    operand(classOf[StreamExecExchange],
      operand(classOf[RelNode], any))),
  "TwoStageOptimizedAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val tableConfig = call.getPlanner.getContext.asInstanceOf[FlinkContext].getTableConfig
    val agg: StreamExecGroupAggregate = call.rel(0)
    val realInput: RelNode = call.rel(2)

    val needRetraction = StreamExecRetractionRules.isAccRetract(realInput)

    // TODO supports RelModifiedMonotonicity
    val needRetractionArray = AggregateUtil.getNeedRetractions(
      agg.grouping.length, needRetraction, agg.aggCalls)

    val aggInfoList = AggregateUtil.transformToStreamAggregateInfoList(
      agg.aggCalls,
      agg.getInput.getRowType,
      needRetractionArray,
      needRetraction,
      isStateBackendDataViews = true)

    val isMiniBatchEnabled = tableConfig.getConf.getLong(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY) > 0
    val isTwoPhaseEnabled = !tableConfig.getConf
      .getString(PlannerConfigOptions.SQL_OPTIMIZER_AGG_PHASE_ENFORCER)
      .equalsIgnoreCase(AggPhaseEnforcer.ONE_PHASE.toString)

    isMiniBatchEnabled && isTwoPhaseEnabled &&
      AggregateUtil.doAllSupportPartialMerge(aggInfoList.aggInfos) &&
      !isInputSatisfyRequiredDistribution(realInput, agg.grouping)
  }

  private def isInputSatisfyRequiredDistribution(input: RelNode, keys: Array[Int]): Boolean = {
    val requiredDistribution = createDistribution(keys)
    val inputDistribution = input.getTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    inputDistribution.satisfies(requiredDistribution)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg: StreamExecGroupAggregate = call.rel(0)
    val realInput: RelNode = call.rel(2)
    val needRetraction = StreamExecRetractionRules.isAccRetract(realInput)

    // TODO supports RelModifiedMonotonicity
    val needRetractionArray = AggregateUtil.getNeedRetractions(
      agg.grouping.length, needRetraction, agg.aggCalls)

    val localAggInfoList = AggregateUtil.transformToStreamAggregateInfoList(
      agg.aggCalls,
      realInput.getRowType,
      needRetractionArray,
      needRetraction,
      isStateBackendDataViews = false)

    val globalAggInfoList = AggregateUtil.transformToStreamAggregateInfoList(
      agg.aggCalls,
      realInput.getRowType,
      needRetractionArray,
      needRetraction,
      isStateBackendDataViews = true)

    val globalHashAgg = createTwoStageAgg(realInput, localAggInfoList, globalAggInfoList, agg)
    call.transformTo(globalHashAgg)
  }

  // the difference between localAggInfos and aggInfos is local agg use heap dataview,
  // but global agg use state dataview
  private def createTwoStageAgg(
      input: RelNode,
      localAggInfoList: AggregateInfoList,
      globalAggInfoList: AggregateInfoList,
      agg: StreamExecGroupAggregate): StreamExecGlobalGroupAggregate = {
    val localAggRowType = AggregateUtil.inferLocalAggRowType(
      localAggInfoList,
      input.getRowType,
      agg.grouping,
      input.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory])

    // local agg shouldn't produce AccRetract Message
    val localAggTraitSet = input.getTraitSet.plus(AccModeTrait(AccMode.Acc))
    val localHashAgg = new StreamExecLocalGroupAggregate(
      agg.getCluster,
      localAggTraitSet,
      input,
      localAggRowType,
      agg.grouping,
      agg.aggCalls,
      localAggInfoList,
      agg.partialFinalType)

    // grouping keys is forwarded by local agg, use indices instead of groupings
    val globalGrouping = agg.grouping.indices.toArray
    val globalDistribution = createDistribution(globalGrouping)
    // create exchange if needed
    val newInput = satisfyDistribution(
      FlinkConventions.STREAM_PHYSICAL, localHashAgg, globalDistribution)
    val globalAggProvidedTraitSet = agg.getTraitSet

    new StreamExecGlobalGroupAggregate(
      agg.getCluster,
      globalAggProvidedTraitSet,
      newInput,
      input.getRowType,
      agg.getRowType,
      globalGrouping,
      localAggInfoList,
      globalAggInfoList,
      agg.partialFinalType)
  }

  private def createDistribution(keys: Array[Int]): FlinkRelDistribution = {
    if (keys.nonEmpty) {
      val fields = new util.ArrayList[Integer]()
      keys.foreach(fields.add(_))
      FlinkRelDistribution.hash(fields)
    } else {
      FlinkRelDistribution.SINGLETON
    }
  }

}

object TwoStageOptimizedAggregateRule {
  val INSTANCE: RelOptRule = new TwoStageOptimizedAggregateRule
}
