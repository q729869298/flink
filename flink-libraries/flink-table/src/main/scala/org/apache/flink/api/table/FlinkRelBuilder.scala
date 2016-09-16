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

package org.apache.flink.api.table

import java.util.Collections

import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan._
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.tools.{FrameworkConfig, RelBuilder}

/**
  * Flink specific [[RelBuilder]] that changes the default type factory to a [[FlinkTypeFactory]].
  */
class FlinkRelBuilder(
    context: Context,
    relOptCluster: RelOptCluster,
    relOptSchema: RelOptSchema)
  extends RelBuilder(
    context,
    relOptCluster,
    relOptSchema) {

  def getPlanner: RelOptPlanner = cluster.getPlanner

  def getCluster = cluster

  override def getTypeFactory: FlinkTypeFactory =
    super.getTypeFactory.asInstanceOf[FlinkTypeFactory]
}

object FlinkRelBuilder {

  def create(config: FrameworkConfig): FlinkRelBuilder = {

    // create Flink type factory
    val typeSystem = config.getTypeSystem
    val typeFactory = new FlinkTypeFactory(typeSystem)

    // create context instances with Flink type factory
    val planner = new VolcanoPlanner(Contexts.empty())
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE)
    val cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory))
    val calciteSchema = CalciteSchema.from(config.getDefaultSchema)
    val relOptSchema = new CalciteCatalogReader(
      calciteSchema,
      config.getParserConfig.caseSensitive(),
      Collections.emptyList(),
      typeFactory)

    new FlinkRelBuilder(config.getContext, cluster, relOptSchema)
  }

}
