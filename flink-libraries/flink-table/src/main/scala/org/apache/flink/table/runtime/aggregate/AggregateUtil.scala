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

import java.util

import org.apache.calcite.rel.`type`._
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.{SqlAggFunction, SqlKind}
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.fun._
import org.apache.flink.api.common.functions.{MapFunction, RichGroupReduceFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.calcite.{FlinkRelBuilder, FlinkTypeFactory}
import FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.typeutils.TypeCheckUtils._
import org.apache.flink.streaming.api.functions.windowing.{AllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.windows.{Window => DataStreamWindow}
import org.apache.flink.table.api.{TableException, Types}
import org.apache.flink.table.typeutils.{RowIntervalTypeInfo, TimeIntervalTypeInfo}
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object AggregateUtil {

  type CalcitePair[T, R] = org.apache.calcite.util.Pair[T, R]
  type JavaList[T] = java.util.List[T]

  /**
    * Create a [[org.apache.flink.api.common.functions.MapFunction]] that prepares for aggregates.
    * The function returns intermediate aggregate values of all aggregate function which are
    * organized by the following format:
    *
    * {{{
    *                   avg(x) aggOffsetInRow = 2          count(z) aggOffsetInRow = 5
    *                             |                          |
    *                             v                          v
    *        +---------+---------+--------+--------+--------+--------+
    *        |groupKey1|groupKey2|  sum1  | count1 |  sum2  | count2 |
    *        +---------+---------+--------+--------+--------+--------+
    *                                              ^
    *                                              |
    *                               sum(y) aggOffsetInRow = 4
    * }}}
    *
    */
  private[flink] def createPrepareMapFunction(
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    groupings: Array[Int],
    inputType: RelDataType): MapFunction[Any, Row] = {

    val (aggFieldIndexes,aggregates) = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),
      inputType,
      groupings.length)

    val mapReturnType: RowTypeInfo =
      createAggregateBufferDataType(groupings, aggregates, inputType)

    val mapFunction = new AggregateMapFunction[Row, Row](
      aggregates,
      aggFieldIndexes,
      groupings,
      mapReturnType.asInstanceOf[RowTypeInfo]).asInstanceOf[MapFunction[Any, Row]]

    mapFunction
  }


  /**
    * Create a [[org.apache.flink.api.common.functions.MapFunction]] that prepares for aggregates.
    * The output of the function contains the grouping keys and the timestamp and the intermediate
    * aggregate values of all aggregate function. The timestamp field is aligned to time window
    * start and used to be a grouping key in case of time window. In case of count window on
    * event-time, the timestamp is not aligned and used to sort.
    *
    * The output is stored in Row by the following format:
    *
    * {{{
    *                      avg(x) aggOffsetInRow = 2      count(z) aggOffsetInRow = 5
    *                            |                          |
    *                            v                          v
    *        +---------+---------+--------+--------+--------+--------+--------+
    *        |groupKey1|groupKey2|  sum1  | count1 |  sum2  | count2 | rowtime|
    *        +---------+---------+--------+--------+--------+--------+--------+
    *                                              ^                 ^
    *                                              |                 |
    *                                 sum(y) aggOffsetInRow = 4    rowtime to group or sort
    * }}}
    *
    * NOTE: this function is only used for time based window on batch tables.
    */
  def createDataSetWindowPrepareMapFunction(
    window: LogicalWindow,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    groupings: Array[Int],
    inputType: RelDataType): MapFunction[Any, Row] = {

    val (aggFieldIndexes, aggregates) = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),
      inputType,
      groupings.length)

    val mapReturnType: RowTypeInfo =
      createAggregateBufferDataType(groupings, aggregates, inputType, Some(Types.LONG))

    val (timeFieldPos, tumbleTimeWindowSize) = window match {
      case EventTimeTumblingGroupWindow(_, time, size) =>
        (getTimeFieldPosition(time, inputType), Some(asLong(size)))
      case EventTimeSessionGroupWindow(_, time, _) =>
        (getTimeFieldPosition(time, inputType), None)
      case _ =>
        throw new UnsupportedOperationException(s"$window is currently not supported on batch")
    }

    new DataSetWindowAggregateMapFunction(
      aggregates,
      aggFieldIndexes,
      groupings,
      timeFieldPos,
      tumbleTimeWindowSize,
      mapReturnType).asInstanceOf[MapFunction[Any, Row]]
  }

  /**
    * Create a [[org.apache.flink.api.common.functions.GroupReduceFunction]] to compute window
    * aggregates on batch tables. If all aggregates support partial aggregation and is a time
    * window, the [[org.apache.flink.api.common.functions.GroupReduceFunction]] implements
    * [[org.apache.flink.api.common.functions.CombineFunction]] as well.
    *
    * NOTE: this function is only used for window on batch tables.
    */
  def createDataSetWindowAggGroupReduceFunction(
    window: LogicalWindow,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    inputType: RelDataType,
    outputType: RelDataType,
    groupings: Array[Int],
    properties: Seq[NamedWindowProperty]): RichGroupReduceFunction[Row, Row] = {

    val aggregates = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),
      inputType,
      groupings.length)._2

    // the addition one field is used to store time attribute
    val intermediateRowArity = groupings.length +
      aggregates.map(_.intermediateDataType.length).sum + 1

    // the mapping relation between field index of intermediate aggregate Row and output Row.
    val groupingOffsetMapping = getGroupKeysMapping(inputType, outputType, groupings)

    // the mapping relation between aggregate function index in list and its corresponding
    // field index in output Row.
    val aggOffsetMapping = getAggregateMapping(namedAggregates, outputType)

    if (groupingOffsetMapping.length != groupings.length ||
      aggOffsetMapping.length != namedAggregates.length) {
      throw new TableException(
        "Could not find output field in input data type " +
          "or aggregate functions.")
    }

    window match {
      case EventTimeTumblingGroupWindow(_, _, size) if isTimeInterval(size.resultType) =>
        // tumbling time window
        val (startPos, endPos) = computeWindowStartEndPropertyPos(properties)
        if (aggregates.forall(_.supportPartial)) {
          // for incremental aggregations
          new DataSetTumbleTimeWindowAggReduceCombineFunction(
            intermediateRowArity - 1,
            asLong(size),
            startPos,
            endPos,
            aggregates,
            groupingOffsetMapping,
            aggOffsetMapping,
            intermediateRowArity,
            outputType.getFieldCount)
        }
        else {
          // for non-incremental aggregations
          new DataSetTumbleTimeWindowAggReduceGroupFunction(
            intermediateRowArity - 1,
            asLong(size),
            startPos,
            endPos,
            aggregates,
            groupingOffsetMapping,
            aggOffsetMapping,
            intermediateRowArity,
            outputType.getFieldCount)
        }
      case EventTimeTumblingGroupWindow(_, _, size) =>
        // tumbling count window
        new DataSetTumbleCountWindowAggReduceGroupFunction(
          asLong(size),
          aggregates,
          groupingOffsetMapping,
          aggOffsetMapping,
          intermediateRowArity,
          outputType.getFieldCount)
      case _ =>
        throw new UnsupportedOperationException(s"$window is currently not supported on batch")
    }
  }

  /**
    * Create a [[org.apache.flink.api.common.functions.GroupReduceFunction]] to compute aggregates.
    * If all aggregates support partial aggregation, the
    * [[org.apache.flink.api.common.functions.GroupReduceFunction]] implements
    * [[org.apache.flink.api.common.functions.CombineFunction]] as well.
    *
    */
  private[flink] def createAggregateGroupReduceFunction(
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    inputType: RelDataType,
    outputType: RelDataType,
    groupings: Array[Int]): RichGroupReduceFunction[Row, Row] = {

    val aggregates = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),
      inputType,
      groupings.length)._2

    val (groupingOffsetMapping, aggOffsetMapping) =
      getGroupingOffsetAndAggOffsetMapping(
        namedAggregates,
        inputType,
        outputType,
        groupings)

    val allPartialAggregate: Boolean = aggregates.forall(_.supportPartial)

    val intermediateRowArity = groupings.length +
      aggregates.map(_.intermediateDataType.length).sum

    val groupReduceFunction =
      if (allPartialAggregate) {
        new AggregateReduceCombineFunction(
          aggregates,
          groupingOffsetMapping,
          aggOffsetMapping,
          intermediateRowArity,
          outputType.getFieldCount)
      }
      else {
        new AggregateReduceGroupFunction(
          aggregates,
          groupingOffsetMapping,
          aggOffsetMapping,
          intermediateRowArity,
          outputType.getFieldCount)
      }
    groupReduceFunction
  }

  /**
    * Create a [[org.apache.flink.api.common.functions.ReduceFunction]] for incremental window
    * aggregation.
    *
    */
  private[flink] def createIncrementalAggregateReduceFunction(
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    inputType: RelDataType,
    outputType: RelDataType,
    groupings: Array[Int]): IncrementalAggregateReduceFunction = {

    val aggregates = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),inputType,groupings.length)._2

    val groupingOffsetMapping =
      getGroupingOffsetAndAggOffsetMapping(
        namedAggregates,
        inputType,
        outputType,
        groupings)._1

    val intermediateRowArity = groupings.length + aggregates.map(_.intermediateDataType.length).sum
    val reduceFunction = new IncrementalAggregateReduceFunction(
      aggregates,
      groupingOffsetMapping,
      intermediateRowArity)
    reduceFunction
  }

  /**
    * Create an [[AllWindowFunction]] to compute non-partitioned group window aggregates.
    */
  private[flink] def createAllWindowAggregationFunction(
    window: LogicalWindow,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    inputType: RelDataType,
    outputType: RelDataType,
    groupings: Array[Int],
    properties: Seq[NamedWindowProperty])
  : AllWindowFunction[Row, Row, DataStreamWindow] = {

    val aggFunction =
      createAggregateGroupReduceFunction(
        namedAggregates,
        inputType,
        outputType,
        groupings)

    if (isTimeWindow(window)) {
      val (startPos, endPos) = computeWindowStartEndPropertyPos(properties)
      new AggregateAllTimeWindowFunction(aggFunction, startPos, endPos)
      .asInstanceOf[AllWindowFunction[Row, Row, DataStreamWindow]]
    } else {
      new AggregateAllWindowFunction(aggFunction)
    }
  }

  /**
    * Create a [[WindowFunction]] to compute partitioned group window aggregates.
    *
    */
  private[flink] def createWindowAggregationFunction(
    window: LogicalWindow,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    inputType: RelDataType,
    outputType: RelDataType,
    groupings: Array[Int],
    properties: Seq[NamedWindowProperty])
  : WindowFunction[Row, Row, Tuple, DataStreamWindow] = {

    val aggFunction =
      createAggregateGroupReduceFunction(
        namedAggregates,
        inputType,
        outputType,
        groupings)

    if (isTimeWindow(window)) {
      val (startPos, endPos) = computeWindowStartEndPropertyPos(properties)
      new AggregateTimeWindowFunction(aggFunction, startPos, endPos)
      .asInstanceOf[WindowFunction[Row, Row, Tuple, DataStreamWindow]]
    } else {
      new AggregateWindowFunction(aggFunction)
    }
  }

  /**
    * Create an [[AllWindowFunction]] to finalize incrementally pre-computed non-partitioned
    * window aggreagtes.
    */
  private[flink] def createAllWindowIncrementalAggregationFunction(
    window: LogicalWindow,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    inputType: RelDataType,
    outputType: RelDataType,
    groupings: Array[Int],
    properties: Seq[NamedWindowProperty]): AllWindowFunction[Row, Row, DataStreamWindow] = {

    val aggregates = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),inputType,groupings.length)._2

    val (groupingOffsetMapping, aggOffsetMapping) =
      getGroupingOffsetAndAggOffsetMapping(
      namedAggregates,
      inputType,
      outputType,
      groupings)

    val finalRowArity = outputType.getFieldCount

    if (isTimeWindow(window)) {
      val (startPos, endPos) = computeWindowStartEndPropertyPos(properties)
      new IncrementalAggregateAllTimeWindowFunction(
        aggregates,
        groupingOffsetMapping,
        aggOffsetMapping,
        finalRowArity,
        startPos,
        endPos)
      .asInstanceOf[AllWindowFunction[Row, Row, DataStreamWindow]]
    } else {
      new IncrementalAggregateAllWindowFunction(
        aggregates,
        groupingOffsetMapping,
        aggOffsetMapping,
        finalRowArity)
    }
  }

  /**
    * Create a [[WindowFunction]] to finalize incrementally pre-computed window aggregates.
    */
  private[flink] def createWindowIncrementalAggregationFunction(
    window: LogicalWindow,
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    inputType: RelDataType,
    outputType: RelDataType,
    groupings: Array[Int],
    properties: Seq[NamedWindowProperty]): WindowFunction[Row, Row, Tuple, DataStreamWindow] = {

    val aggregates = transformToAggregateFunctions(
      namedAggregates.map(_.getKey),inputType,groupings.length)._2

    val (groupingOffsetMapping, aggOffsetMapping) =
      getGroupingOffsetAndAggOffsetMapping(
        namedAggregates,
        inputType,
        outputType,
        groupings)

    val finalRowArity = outputType.getFieldCount

    if (isTimeWindow(window)) {
      val (startPos, endPos) = computeWindowStartEndPropertyPos(properties)
      new IncrementalAggregateTimeWindowFunction(
        aggregates,
        groupingOffsetMapping,
        aggOffsetMapping,
        finalRowArity,
        startPos,
        endPos)
      .asInstanceOf[WindowFunction[Row, Row, Tuple, DataStreamWindow]]
    } else {
      new IncrementalAggregateWindowFunction(
        aggregates,
        groupingOffsetMapping,
        aggOffsetMapping,
        finalRowArity)
    }
  }

  /**
    * Return true if all aggregates can be partially computed. False otherwise.
    */
  private[flink] def doAllSupportPartialAggregation(
    aggregateCalls: Seq[AggregateCall],
    inputType: RelDataType,
    groupKeysCount: Int): Boolean = {
    transformToAggregateFunctions(
      aggregateCalls,
      inputType,
      groupKeysCount)._2.forall(_.supportPartial)
  }

  /**
    * @return groupingOffsetMapping (mapping relation between field index of intermediate
    *         aggregate Row and output Row.)
    *         and aggOffsetMapping (the mapping relation between aggregate function index in list
    *         and its corresponding field index in output Row.)
    */
  private def getGroupingOffsetAndAggOffsetMapping(
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    inputType: RelDataType,
    outputType: RelDataType,
    groupings: Array[Int]): (Array[(Int, Int)], Array[(Int, Int)]) = {

    // the mapping relation between field index of intermediate aggregate Row and output Row.
    val groupingOffsetMapping = getGroupKeysMapping(inputType, outputType, groupings)

    // the mapping relation between aggregate function index in list and its corresponding
    // field index in output Row.
    val aggOffsetMapping = getAggregateMapping(namedAggregates, outputType)

    if (groupingOffsetMapping.length != groupings.length ||
      aggOffsetMapping.length != namedAggregates.length) {
      throw new TableException(
        "Could not find output field in input data type " +
          "or aggregate functions.")
    }
    (groupingOffsetMapping, aggOffsetMapping)
  }

  private def isTimeWindow(window: LogicalWindow) = {
    window match {
      case ProcessingTimeTumblingGroupWindow(_, size) => isTimeInterval(size.resultType)
      case ProcessingTimeSlidingGroupWindow(_, size, _) => isTimeInterval(size.resultType)
      case ProcessingTimeSessionGroupWindow(_, _) => true
      case EventTimeTumblingGroupWindow(_, _, size) => isTimeInterval(size.resultType)
      case EventTimeSlidingGroupWindow(_, _, size, _) => isTimeInterval(size.resultType)
      case EventTimeSessionGroupWindow(_, _, _) => true
    }
  }

  private[flink] def computeWindowStartEndPropertyPos(
    properties: Seq[NamedWindowProperty]): (Option[Int], Option[Int]) = {

    val propPos = properties.foldRight((None: Option[Int], None: Option[Int], 0)) {
      (p, x) => p match {
        case NamedWindowProperty(name, prop) =>
          prop match {
            case WindowStart(_) if x._1.isDefined =>
              throw new TableException("Duplicate WindowStart property encountered. This is a bug.")
            case WindowStart(_) =>
              (Some(x._3), x._2, x._3 - 1)
            case WindowEnd(_) if x._2.isDefined =>
              throw new TableException("Duplicate WindowEnd property encountered. This is a bug.")
            case WindowEnd(_) =>
              (x._1, Some(x._3), x._3 - 1)
          }
      }
    }
    (propPos._1, propPos._2)
  }

  private def transformToAggregateFunctions(
    aggregateCalls: Seq[AggregateCall],
    inputType: RelDataType,
    groupKeysCount: Int): (Array[Int], Array[Aggregate[_ <: Any]]) = {

    // store the aggregate fields of each aggregate function, by the same order of aggregates.
    val aggFieldIndexes = new Array[Int](aggregateCalls.size)
    val aggregates = new Array[Aggregate[_ <: Any]](aggregateCalls.size)

    // set the start offset of aggregate buffer value to group keys' length, 
    // as all the group keys would be moved to the start fields of intermediate
    // aggregate data.
    var aggOffset = groupKeysCount

    // create aggregate function instances by function type and aggregate field data type.
    aggregateCalls.zipWithIndex.foreach { case (aggregateCall, index) =>
      val argList: util.List[Integer] = aggregateCall.getArgList
      if (argList.isEmpty) {
        if (aggregateCall.getAggregation.isInstanceOf[SqlCountAggFunction]) {
          aggFieldIndexes(index) = 0
        } else {
          throw new TableException("Aggregate fields should not be empty.")
        }
      } else {
        if (argList.size() > 1) {
          throw new TableException("Currently, do not support aggregate on multi fields.")
        }
        aggFieldIndexes(index) = argList.get(0)
      }
      val sqlTypeName = inputType.getFieldList.get(aggFieldIndexes(index)).getType.getSqlTypeName
      aggregateCall.getAggregation match {
        case _: SqlSumAggFunction | _: SqlSumEmptyIsZeroAggFunction => {
          aggregates(index) = sqlTypeName match {
            case TINYINT =>
              new ByteSumAggregate
            case SMALLINT =>
              new ShortSumAggregate
            case INTEGER =>
              new IntSumAggregate
            case BIGINT =>
              new LongSumAggregate
            case FLOAT =>
              new FloatSumAggregate
            case DOUBLE =>
              new DoubleSumAggregate
            case DECIMAL =>
              new DecimalSumAggregate
            case sqlType: SqlTypeName =>
              throw new TableException("Sum aggregate does no support type:" + sqlType)
          }
        }
        case _: SqlAvgAggFunction => {
          aggregates(index) = sqlTypeName match {
            case TINYINT =>
               new ByteAvgAggregate
            case SMALLINT =>
              new ShortAvgAggregate
            case INTEGER =>
              new IntAvgAggregate
            case BIGINT =>
              new LongAvgAggregate
            case FLOAT =>
              new FloatAvgAggregate
            case DOUBLE =>
              new DoubleAvgAggregate
            case DECIMAL =>
              new DecimalAvgAggregate
            case sqlType: SqlTypeName =>
              throw new TableException("Avg aggregate does no support type:" + sqlType)
          }
        }
        case sqlMinMaxFunction: SqlMinMaxAggFunction => {
          aggregates(index) = if (sqlMinMaxFunction.getKind == SqlKind.MIN) {
            sqlTypeName match {
              case TINYINT =>
                new ByteMinAggregate
              case SMALLINT =>
                new ShortMinAggregate
              case INTEGER =>
                new IntMinAggregate
              case BIGINT =>
                new LongMinAggregate
              case FLOAT =>
                new FloatMinAggregate
              case DOUBLE =>
                new DoubleMinAggregate
              case DECIMAL =>
                new DecimalMinAggregate
              case BOOLEAN =>
                new BooleanMinAggregate
              case sqlType: SqlTypeName =>
                throw new TableException("Min aggregate does no support type:" + sqlType)
            }
          } else {
            sqlTypeName match {
              case TINYINT =>
                new ByteMaxAggregate
              case SMALLINT =>
                new ShortMaxAggregate
              case INTEGER =>
                new IntMaxAggregate
              case BIGINT =>
                new LongMaxAggregate
              case FLOAT =>
                new FloatMaxAggregate
              case DOUBLE =>
                new DoubleMaxAggregate
              case DECIMAL =>
                new DecimalMaxAggregate
              case BOOLEAN =>
                new BooleanMaxAggregate
              case sqlType: SqlTypeName =>
                throw new TableException("Max aggregate does no support type:" + sqlType)
            }
          }
        }
        case _: SqlCountAggFunction =>
          aggregates(index) = new CountAggregate
        case unSupported: SqlAggFunction =>
          throw new TableException("unsupported Function: " + unSupported.getName)
      }
      setAggregateDataOffset(index)
    }

    // set the aggregate intermediate data start index in Row, and update current value.
    def setAggregateDataOffset(index: Int): Unit = {
      aggregates(index).setAggOffsetInRow(aggOffset)
      aggOffset += aggregates(index).intermediateDataType.length
    }

    (aggFieldIndexes, aggregates)
  }

  private def createAggregateBufferDataType(
    groupings: Array[Int],
    aggregates: Array[Aggregate[_]],
    inputType: RelDataType,
    windowKeyType: Option[TypeInformation[_]] = None): RowTypeInfo = {

    // get the field data types of group keys.
    val groupingTypes: Seq[TypeInformation[_]] = groupings
      .map(inputType.getFieldList.get(_).getType)
      .map(FlinkTypeFactory.toTypeInfo)

    // get all field data types of all intermediate aggregates
    val aggTypes: Seq[TypeInformation[_]] = aggregates.flatMap(_.intermediateDataType)

    // concat group key types and aggregation types
    val allFieldTypes = groupingTypes ++: aggTypes ++: windowKeyType
    val partialType = new RowTypeInfo(allFieldTypes.toSeq: _*)
    partialType
  }

  // Find the mapping between the index of aggregate list and aggregated value index in output Row.
  private def getAggregateMapping(
    namedAggregates: Seq[CalcitePair[AggregateCall, String]],
    outputType: RelDataType): Array[(Int, Int)] = {

    // the mapping relation between aggregate function index in list and its corresponding
    // field index in output Row.
    var aggOffsetMapping = ArrayBuffer[(Int, Int)]()

    outputType.getFieldList.zipWithIndex.foreach{
      case (outputFieldType, outputIndex) =>
        namedAggregates.zipWithIndex.foreach {
          case (namedAggCall, aggregateIndex) =>
            if (namedAggCall.getValue.equals(outputFieldType.getName) &&
                namedAggCall.getKey.getType.equals(outputFieldType.getType)) {
              aggOffsetMapping += ((outputIndex, aggregateIndex))
            }
        }
    }
   
    aggOffsetMapping.toArray
  }

  // Find the mapping between the index of group key in intermediate aggregate Row and its index
  // in output Row.
  private def getGroupKeysMapping(
    inputDatType: RelDataType,
    outputType: RelDataType,
    groupKeys: Array[Int]): Array[(Int, Int)] = {

    // the mapping relation between field index of intermediate aggregate Row and output Row.
    var groupingOffsetMapping = ArrayBuffer[(Int, Int)]()

    outputType.getFieldList.zipWithIndex.foreach {
      case (outputFieldType, outputIndex) =>
        inputDatType.getFieldList.zipWithIndex.foreach {
          // find the field index in input data type.
          case (inputFieldType, inputIndex) =>
            if (outputFieldType.getName.equals(inputFieldType.getName) &&
                outputFieldType.getType.equals(inputFieldType.getType)) {
              // as aggregated field in output data type would not have a matched field in
              // input data, so if inputIndex is not -1, it must be a group key. Then we can
              // find the field index in buffer data by the group keys index mapping between
              // input data and buffer data.
              for (i <- groupKeys.indices) {
                if (inputIndex == groupKeys(i)) {
                  groupingOffsetMapping += ((outputIndex, i))
                }
              }
            }
        }
    }

    groupingOffsetMapping.toArray
  }

  private def getTimeFieldPosition(timeField: Expression, inputType: RelDataType): Int = {
    timeField match {
      case ResolvedFieldReference(name, resultType) =>
        // get the RelDataType referenced by the time-field
        val relDataType = inputType.getFieldList.filter(r => name.equals(r.getName))
        // should only match one
        if (relDataType.length == 1) {
          relDataType.head.getIndex
        } else {
          throw new IllegalArgumentException()
        }
      case _ => throw new IllegalArgumentException()
    }
  }

  private def asLong(expr: Expression): Long = expr match {
    case Literal(value: Long, TimeIntervalTypeInfo.INTERVAL_MILLIS) => value
    case Literal(value: Long, RowIntervalTypeInfo.INTERVAL_ROWS) => value
    case _ => throw new IllegalArgumentException()
  }

}

