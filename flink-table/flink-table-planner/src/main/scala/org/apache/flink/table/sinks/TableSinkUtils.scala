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

package org.apache.flink.table.sinks

import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.operations.QueryOperation

import java.util.{List => JList, Map => JMap}

import collection.JavaConversions._

object TableSinkUtils {

  /**
    * Checks if the given [[QueryOperation]] can be written to the given [[TableSink]].
    * It checks if the names & the field types match. If this sink is a [[PartitionableTableSink]],
    * it will also validate the partitions.
    *
    * @param staticPartitions Static partitions of the sink if there exists any.
    * @param query            The query that is supposed to be written.
    * @param sinkPath         Tha path of the sink. It is needed just for logging. It does not
    *                         participate in the validation.
    * @param sink             The sink that we want to write to.
    */
  def validateSink(
      staticPartitions: JMap[String, String],
      query: QueryOperation,
      sinkPath: JList[String],
      sink: TableSink[_])
    : Unit = {
    // validate schema of source table and table sink
    val srcFieldTypes = query.getTableSchema.getFieldTypes
    val sinkFieldTypes = sink.getTableSchema.getFieldTypes

    if (srcFieldTypes.length != sinkFieldTypes.length ||
      srcFieldTypes.zip(sinkFieldTypes).exists { case (srcF, snkF) => srcF != snkF }) {

      val srcFieldNames = query.getTableSchema.getFieldNames
      val sinkFieldNames = sink.getTableSchema.getFieldNames

      // format table and table sink schema strings
      val srcSchema = srcFieldNames.zip(srcFieldTypes)
        .map { case (n, t) => s"$n: ${t.getTypeClass.getSimpleName}" }
        .mkString("[", ", ", "]")
      val sinkSchema = sinkFieldNames.zip(sinkFieldTypes)
        .map { case (n, t) => s"$n: ${t.getTypeClass.getSimpleName}" }
        .mkString("[", ", ", "]")

      throw new ValidationException(
        s"Field types of query result and registered TableSink " +
          s"${sinkPath} do not match.\n" +
          s"Query result schema: $srcSchema\n" +
          s"TableSink schema:    $sinkSchema")
    }
    // check partitions are valid
    sink match {
      case partitionableTableSink: PartitionableTableSink =>
        val partitionFieldList = partitionableTableSink.getPartitionFieldNames
        val partitionFields: JList[String] = if (partitionFieldList == null) {
          List()
        } else {
          partitionFieldList
        }
        staticPartitions.map(_._1) foreach { p =>
          if (!partitionFields.contains(p)) {
            throw new TableException(s"Static partition column $p " +
              s"should be in the partition fields list $partitionFields.")
          }
        }
        staticPartitions.map(_._1) zip partitionFields.slice(0, staticPartitions.size()) foreach {
          case (p1, p2) =>
            if (p1 != p2) {
              if (!partitionFields.contains(p1)) {
                throw new TableException(s"Static partition column $p1 " +
                  s"should be in the partition fields list $partitionFields.")
              } else {
                throw new TableException(s"Static partition column $p1 " +
                  s"should appear before dynamic partition $p2.")
              }
            }
        }
      case _ =>
    }
  }
}
