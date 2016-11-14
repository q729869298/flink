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

package org.apache.flink.api.table.plan.nodes.dataset

import org.apache.calcite.plan._

class DataSetConvention extends Convention {

  override def toString: String = getName

  override def useAbstractConvertersForConversion(
    fromTraits: RelTraitSet,
    toTraits: RelTraitSet): Boolean = false

  override def canConvertConvention(toConvention: Convention): Boolean = false

  def getInterface: Class[_] = classOf[DataSetRel]

  def getName: String = "DATASET"

  def getTraitDef: RelTraitDef[_ <: RelTrait] = ConventionTraitDef.INSTANCE

  def satisfies(`trait`: RelTrait): Boolean = this eq `trait`

  def register(planner: RelOptPlanner): Unit = { }
}

object DataSetConvention {

  val INSTANCE = new DataSetConvention
}
