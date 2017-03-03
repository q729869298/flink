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

package org.apache.flink.table.runtime.aggregate

import java.lang.Iterable
import java.util.{ArrayList => JArrayList}

import org.apache.flink.api.common.functions.CombineFunction
import org.apache.flink.table.api.TableException
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row

/**
  * It wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]] and
  * [[org.apache.flink.api.java.operators.GroupCombineOperator]]
  *
  * @param aggregates          The aggregate functions.
  * @param groupKeysMapping    The index mapping of group keys between intermediate aggregate Row
  *                            and output Row.
  * @param aggregateMapping    The index mapping between aggregate function list and aggregated
  *                            value
  *                            index in output Row.
  * @param groupingSetsMapping The index mapping of keys in grouping sets between intermediate
  *                            Row and output Row.
  * @param finalRowArity       the arity of the final resulting row
  */
class AggregateReduceCombineFunction(
    private val aggregates: Array[AggregateFunction[_ <: Any]],
    private val groupKeysMapping: Array[(Int, Int)],
    private val aggregateMapping: Array[(Int, Int)],
    private val groupingSetsMapping: Array[(Int, Int)],
    private val finalRowArity: Int)
  extends AggregateReduceGroupFunction(
    aggregates,
    groupKeysMapping,
    aggregateMapping,
    groupingSetsMapping,
    finalRowArity) with CombineFunction[Row, Row] {

  /**
    * For sub-grouped intermediate aggregate Rows, merge all of them into aggregate buffer,
    *
    * @param records Sub-grouped intermediate aggregate Rows iterator.
    * @return Combined intermediate aggregate Row.
    *
    */
  override def combine(records: Iterable[Row]): Row = {

    // merge intermediate aggregate value to buffer.
    var last: Row = null
    accumulatorList.foreach(_.clear())
    for (i <- aggregates.indices) {
      val accumulator = aggregates(i).createAccumulator()
      accumulatorList(i).add(accumulator)
      accumulatorList(i).add(accumulator)
    }

    val iterator = records.iterator()

    while (iterator.hasNext) {
      val record = iterator.next()

      for (i <- aggregates.indices) {
        val newAcc = record.getField(groupKeysMapping.length + i)
          .asInstanceOf[Accumulator]
        accumulatorList(i).set(1, newAcc)
        val retAcc = aggregates(i).merge(accumulatorList(i))
        if (System.identityHashCode(newAcc) == System.identityHashCode(retAcc)) {
          throw TableException(
            "Due to the Object Reuse, it is not safe to use the newACC intance (index = 1) to " +
              "save the merge result. You can change your merge function to use the first " +
              "instance (index = 0) instead.")
        }
        accumulatorList(i).set(0, retAcc)
      }

      last = record
    }

    // set the partial merged result to the aggregateBuffer
    for (i <- aggregates.indices) {
      val agg = aggregates(i)
      aggregateBuffer.setField(groupKeysMapping.length + i, accumulatorList(i).get(0))
    }

    // set group keys to aggregateBuffer.
    for (i <- groupKeysMapping.indices) {
      aggregateBuffer.setField(i, last.getField(i))
    }

    aggregateBuffer
  }
}
