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

package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.api.TableException
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning
import org.apache.flink.table.filesystem.FileSystemTableFactory
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSink
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecSink
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule

import scala.collection.JavaConversions._

class StreamExecSinkRule extends ConverterRule(
    classOf[FlinkLogicalSink],
    FlinkConventions.LOGICAL,
    FlinkConventions.STREAM_PHYSICAL,
    "StreamExecSinkRule") {

  def convert(rel: RelNode): RelNode = {
    val sinkNode = rel.asInstanceOf[FlinkLogicalSink]
    val newTrait = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    var requiredTraitSet = sinkNode.getInput.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    if (sinkNode.catalogTable != null && sinkNode.catalogTable.isPartitioned) {
      sinkNode.tableSink match {
        case partitionSink: SupportsPartitioning =>
          partitionSink.applyStaticPartition(sinkNode.staticPartitions)
          val dynamicPartFields = sinkNode.catalogTable.getPartitionKeys
              .filter(!sinkNode.staticPartitions.contains(_))
          val fieldNames = sinkNode.catalogTable
            .getSchema
            .toPhysicalRowDataType
            .getLogicalType.asInstanceOf[RowType]
            .getFieldNames

          if (dynamicPartFields.nonEmpty) {
            val dynamicPartIndices =
              dynamicPartFields.map(fieldNames.indexOf(_))

            val shuffleEnable = sinkNode
                .catalogTable
                .getOptions
                .get(FileSystemTableFactory.SINK_SHUFFLE_BY_PARTITION.key())

            if (shuffleEnable != null && shuffleEnable.toBoolean) {
              requiredTraitSet = requiredTraitSet.plus(
                FlinkRelDistribution.hash(dynamicPartIndices
                    .map(Integer.valueOf), requireStrict = false))
            }

            if (partitionSink.requiresPartitionGrouping(false)) {
              throw new TableException("Partition grouping in stream mode is not supported yet!")
            }
          }
        case _ => throw new TableException(
          s"'${sinkNode.tableIdentifier.asSummaryString()}' is a partitioned table, " +
            s"but the underlying [${sinkNode.tableSink.asSummaryString()}] DynamicTableSink " +
            s"doesn't implement SupportsPartitioning interface.")
      }
    }

    val newInput = RelOptRule.convert(sinkNode.getInput, requiredTraitSet)

    new StreamExecSink(
      rel.getCluster,
      newTrait,
      newInput,
      sinkNode.tableIdentifier,
      sinkNode.catalogTable,
      sinkNode.tableSink)
  }
}

object StreamExecSinkRule {
  val INSTANCE = new StreamExecSinkRule
}

