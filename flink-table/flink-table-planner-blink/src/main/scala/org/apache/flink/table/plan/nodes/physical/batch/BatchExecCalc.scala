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

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfigOptions}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{CalcCodeGenerator, CodeGeneratorContext}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef, TraitUtil}
import org.apache.flink.table.plan.nodes.common.CommonCalc
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.plan.util.RelExplainUtil

import org.apache.calcite.plan._
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rex.{RexCall, RexInputRef, RexProgram}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.mapping.{Mapping, MappingType, Mappings}

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for [[Calc]].
  */
class BatchExecCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    calcProgram: RexProgram,
    outputRowType: RelDataType)
  extends CommonCalc(cluster, traitSet, inputRel, calcProgram)
  with BatchPhysicalRel
  with BatchExecNode[BaseRow] {

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new BatchExecCalc(cluster, traitSet, child, program, outputRowType)
  }

  override def satisfyTraits(requiredTraitSet: RelTraitSet): RelNode = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    // Does not push broadcast distribution trait down into Calc.
    if (requiredDistribution.getType == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
      return null
    }
    val projects = calcProgram.getProjectList.map(calcProgram.expandLocalRef)

    def getProjectMapping: Mapping = {
      val mapping = Mappings.create(MappingType.INVERSE_FUNCTION,
        getInput.getRowType.getFieldCount, projects.size)
      projects.zipWithIndex.foreach {
        case (project, index) =>
          project match {
            case inputRef: RexInputRef => mapping.set(inputRef.getIndex, index)
            case call: RexCall if call.getKind == SqlKind.AS =>
              call.getOperands.head match {
                case inputRef: RexInputRef => mapping.set(inputRef.getIndex, index)
                case _ => // ignore
              }
            case _ => // ignore
          }
      }
      mapping.inverse()
    }

    val mapping = getProjectMapping
    val appliedDistribution = requiredDistribution.apply(mapping)
    // If both distribution and collation can be satisfied, satisfy both. If only distribution
    // can be satisfied, only satisfy distribution. There is no possibility to only satisfy
    // collation here except for there is no distribution requirement.
    if ((!requiredDistribution.isTop) && (appliedDistribution eq FlinkRelDistribution.ANY)) {
      return null
    }

    val requiredCollation = requiredTraitSet.getTrait(RelCollationTraitDef.INSTANCE)
    val appliedCollation = TraitUtil.apply(requiredCollation, mapping)
    val canCollationPushedDown = !appliedCollation.getFieldCollations.isEmpty
    // If required traits only contains collation requirements, but collation keys are not columns
    // from input, then no need to satisfy required traits.
    if ((appliedDistribution eq FlinkRelDistribution.ANY) && !canCollationPushedDown) {
      return null
    }

    var inputRequiredTraits = getInput.getTraitSet
    var providedTraits = getTraitSet
    if (!appliedDistribution.isTop) {
      inputRequiredTraits = inputRequiredTraits.replace(appliedDistribution)
      providedTraits = providedTraits.replace(requiredDistribution)
    }
    if (canCollationPushedDown) {
      inputRequiredTraits = inputRequiredTraits.replace(appliedCollation)
      providedTraits = providedTraits.replace(requiredCollation)
    }
    val newInput = RelOptRule.convert(getInput, inputRequiredTraits)
    copy(providedTraits, Seq(newInput))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior = DamBehavior.PIPELINED

  override def getInputNodes: util.List[ExecNode[BatchTableEnvironment, _]] =
    List(getInput.asInstanceOf[ExecNode[BatchTableEnvironment, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    val config = tableEnv.getConfig
    val inputTransform = getInputNodes.get(0).translateToPlan(tableEnv)
        .asInstanceOf[StreamTransformation[BaseRow]]
    val condition = if (calcProgram.getCondition != null) {
      Some(calcProgram.expandLocalRef(calcProgram.getCondition))
    } else {
      None
    }
    val outputType = FlinkTypeFactory.toInternalRowType(getRowType)
    val ctx = CodeGeneratorContext(config)
    val operator = CalcCodeGenerator.generateCalcOperator(
      ctx,
      cluster,
      inputTransform,
      outputType,
      config,
      calcProgram,
      condition,
      opName = "BatchCalc"
    )

    new OneInputTransformation(
      inputTransform,
      RelExplainUtil.calcToString(calcProgram, getExpressionString),
      operator,
      outputType.toTypeInfo,
      config.getConf.getInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM))
  }
}
