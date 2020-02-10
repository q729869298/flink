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
package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.RexNode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, TableException}
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.types.CRow

/**
  * Flink RelNode which matches along with join a python user defined table function.
  */
class DataStreamPythonCorrelate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputSchema: RowSchema,
    input: RelNode,
    scan: FlinkLogicalTableFunctionScan,
    condition: Option[RexNode],
    schema: RowSchema,
    joinSchema: RowSchema,
    joinType: JoinRelType,
    ruleDescription: String)
  extends DataStreamCorrelateBase(
    cluster,
    traitSet,
    inputSchema,
    input,
    scan,
    condition,
    schema,
    joinSchema,
    joinType,
    ruleDescription) {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamPythonCorrelate(
      cluster,
      traitSet,
      inputSchema,
      inputs.get(0),
      scan,
      condition,
      schema,
      joinSchema,
      joinType,
      ruleDescription)
  }

  override def translateToPlan(
      planner: StreamPlanner,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {
    throw new TableException("The implementation will be next commit.")
  }
}
