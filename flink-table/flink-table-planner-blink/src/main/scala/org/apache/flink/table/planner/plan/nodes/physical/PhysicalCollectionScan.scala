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

package org.apache.flink.table.planner.plan.nodes.physical

import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.core.TableScan
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.planner.plan.schema.CollectionTable
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row

import scala.collection.JavaConversions._

/**
  * Base physical RelNode to read data from an external source defined by a
  * java [[java.util.Collection]].
  */
class PhysicalCollectionScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable)
  extends TableScan(cluster, traitSet, table) {

  protected var sourceTransform: Transformation[_] = _

  protected val collectionTable: CollectionTable = table.unwrap(classOf[CollectionTable])

  def getSourceTransformation(env: StreamExecutionEnvironment): Transformation[_] = {
    if (sourceTransform == null) {
      val typeInfo =
        TypeConversions.fromDataTypeToLegacyInfo(collectionTable.dataType)
          .asInstanceOf[TypeInformation[Row]]
      val rows =
        collectionTable.elements.map(_.toSeq).map(r => Row.of(r.asInstanceOf[Seq[Object]]:_*))
      val inputDataStream: DataStream[Row] =
        env.fromCollection(rows, typeInfo).setMaxParallelism(1)
      sourceTransform = inputDataStream.getTransformation
    }
    sourceTransform
  }
}
