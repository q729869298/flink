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

package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.ConcatDistinctAggFunction
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, ROCKSDB_BACKEND, StateBackendMode}
import org.apache.flink.table.planner.runtime.utils.{FailingCollectionSource, StreamingWithStateTestBase, TestData, TestingAppendSink}
import org.apache.flink.table.planner.runtime.utils.TimeTestUtil.TimestampAndWatermarkWithOffset
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy._
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

import java.util
import java.time.ZoneId

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class WindowAggregateITCase(
    aggPhase: AggregatePhaseStrategy,
    state: StateBackendMode,
    useTimestampLtz: Boolean)
  extends StreamingWithStateTestBase(state) {

  // -------------------------------------------------------------------------------
  // Expected output data for TUMBLE WINDOW tests
  // Result of CUBE(name), ROLLUP(name), GROUPING SETS((`name`),()) should be same
  // -------------------------------------------------------------------------------
  val TumbleWindowGroupSetExpectedData = Seq(
    "0,a,2020-10-10T00:00,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
    "0,a,2020-10-10T00:00:05,2020-10-10T00:00:10,1,3.33,null,3.0,1,Comment#2",
    "0,b,2020-10-10T00:00:05,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi",
    "0,b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
    "0,b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3",
    "0,null,2020-10-10T00:00:30,2020-10-10T00:00:35,1,7.77,7.0,7.0,0,null",
    "1,null,2020-10-10T00:00,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
    "1,null,2020-10-10T00:00:05,2020-10-10T00:00:10,3,9.99,6.0,3.0,3,Hello|Hi|Comment#2",
    "1,null,2020-10-10T00:00:15,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
    "1,null,2020-10-10T00:00:30,2020-10-10T00:00:35,2,11.10,7.0,3.0,1,Comment#3"
  )

  val TumbleWindowCubeExpectedData = TumbleWindowGroupSetExpectedData

  val TumbleWindowRollupExpectedData = TumbleWindowGroupSetExpectedData

  // -------------------------------------------------------------------------------
  // Expected output data for HOP WINDOW tests
  // Result of CUBE(name), ROLLUP(name), GROUPING SETS((`name`),()) should be same
  // -------------------------------------------------------------------------------
  val HopWindowGroupSetExpectedData = Seq(
    "0,a,2020-10-09T23:59:55,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
    "0,a,2020-10-10T00:00,2020-10-10T00:00:10,6,19.98,5.0,1.0,3,Comment#2|Hi|Comment#1",
    "0,a,2020-10-10T00:00:05,2020-10-10T00:00:15,1,3.33,null,3.0,1,Comment#2",
    "0,b,2020-10-10T00:00,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi",
    "0,b,2020-10-10T00:00:05,2020-10-10T00:00:15,2,6.66,6.0,3.0,2,Hello|Hi",
    "0,b,2020-10-10T00:00:10,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
    "0,b,2020-10-10T00:00:15,2020-10-10T00:00:25,1,4.44,4.0,4.0,1,Hi",
    "0,b,2020-10-10T00:00:25,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3",
    "0,b,2020-10-10T00:00:30,2020-10-10T00:00:40,1,3.33,3.0,3.0,1,Comment#3",
    "0,null,2020-10-10T00:00:25,2020-10-10T00:00:35,1,7.77,7.0,7.0,0,null",
    "0,null,2020-10-10T00:00:30,2020-10-10T00:00:40,1,7.77,7.0,7.0,0,null",
    "1,null,2020-10-09T23:59:55,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
    "1,null,2020-10-10T00:00,2020-10-10T00:00:10,8,26.64,6.0,1.0,4,Hello|Hi|Comment#2|Comment#1",
    "1,null,2020-10-10T00:00:05,2020-10-10T00:00:15,3,9.99,6.0,3.0,3,Hello|Hi|Comment#2",
    "1,null,2020-10-10T00:00:10,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
    "1,null,2020-10-10T00:00:15,2020-10-10T00:00:25,1,4.44,4.0,4.0,1,Hi",
    "1,null,2020-10-10T00:00:25,2020-10-10T00:00:35,2,11.10,7.0,3.0,1,Comment#3",
    "1,null,2020-10-10T00:00:30,2020-10-10T00:00:40,2,11.10,7.0,3.0,1,Comment#3"
  )
  val HopWindowCubeExpectedData = HopWindowGroupSetExpectedData

  val HopWindowRollupExpectedData = HopWindowGroupSetExpectedData

  // -------------------------------------------------------------------------------
  // Expected output data for CUMULATE WINDOW tests
  // Result of CUBE(name), ROLLUP(name), GROUPING SETS((`name`),()) should be same
  // -------------------------------------------------------------------------------
  val CumulateWindowGroupSetExpectedData = Seq(
    "0,a,2020-10-10T00:00,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
    "0,a,2020-10-10T00:00,2020-10-10T00:00:10,6,19.98,5.0,1.0,3,Hi|Comment#1|Comment#2",
    "0,a,2020-10-10T00:00,2020-10-10T00:00:15,6,19.98,5.0,1.0,3,Hi|Comment#1|Comment#2",
    "0,b,2020-10-10T00:00,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi",
    "0,b,2020-10-10T00:00,2020-10-10T00:00:15,2,6.66,6.0,3.0,2,Hello|Hi",
    "0,b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
    "0,b,2020-10-10T00:00:15,2020-10-10T00:00:25,1,4.44,4.0,4.0,1,Hi",
    "0,b,2020-10-10T00:00:15,2020-10-10T00:00:30,1,4.44,4.0,4.0,1,Hi",
    "0,b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3",
    "0,b,2020-10-10T00:00:30,2020-10-10T00:00:40,1,3.33,3.0,3.0,1,Comment#3",
    "0,b,2020-10-10T00:00:30,2020-10-10T00:00:45,1,3.33,3.0,3.0,1,Comment#3",
    "0,null,2020-10-10T00:00:30,2020-10-10T00:00:35,1,7.77,7.0,7.0,0,null",
    "0,null,2020-10-10T00:00:30,2020-10-10T00:00:40,1,7.77,7.0,7.0,0,null",
    "0,null,2020-10-10T00:00:30,2020-10-10T00:00:45,1,7.77,7.0,7.0,0,null",
    "1,null,2020-10-10T00:00,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
    "1,null,2020-10-10T00:00,2020-10-10T00:00:10,8,26.64,6.0,1.0,4,Hi|Comment#1|Hello|Comment#2",
    "1,null,2020-10-10T00:00,2020-10-10T00:00:15,8,26.64,6.0,1.0,4,Hi|Comment#1|Hello|Comment#2",
    "1,null,2020-10-10T00:00:15,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
    "1,null,2020-10-10T00:00:15,2020-10-10T00:00:25,1,4.44,4.0,4.0,1,Hi",
    "1,null,2020-10-10T00:00:15,2020-10-10T00:00:30,1,4.44,4.0,4.0,1,Hi",
    "1,null,2020-10-10T00:00:30,2020-10-10T00:00:35,2,11.10,7.0,3.0,1,Comment#3",
    "1,null,2020-10-10T00:00:30,2020-10-10T00:00:40,2,11.10,7.0,3.0,1,Comment#3",
    "1,null,2020-10-10T00:00:30,2020-10-10T00:00:45,2,11.10,7.0,3.0,1,Comment#3"
  )

  val CumulateWindowCubeExpectedData = CumulateWindowGroupSetExpectedData

  val CumulateWindowRollupExpectedData = CumulateWindowGroupSetExpectedData

  val SHANGHAI_ZONE = ZoneId.of("Asia/Shanghai")

  @Before
  override def before(): Unit = {
    super.before()
    // enable checkpoint, we are using failing source to force have a complete checkpoint
    // and cover restore path
    env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0))
    FailingCollectionSource.reset()

    val timestampDataId = TestValuesTableFactory.registerData(TestData.windowDataWithTimestamp)
    val timestampLtzDataId = TestValuesTableFactory
      .registerData(TestData.windowDataWithLtzInShanghai)

    tEnv.executeSql(
      s"""
        |CREATE TABLE T1 (
        | `ts` ${if (useTimestampLtz) "BIGINT" else "STRING"},
        | `int` INT,
        | `double` DOUBLE,
        | `float` FLOAT,
        | `bigdec` DECIMAL(10, 2),
        | `string` STRING,
        | `name` STRING,
        | `rowtime` AS
        | ${if (useTimestampLtz) "TO_TIMESTAMP_LTZ(`ts`, 3)" else "TO_TIMESTAMP(`ts`)"},
        | WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND
        |) WITH (
        | 'connector' = 'values',
        | 'data-id' = '${ if (useTimestampLtz) timestampLtzDataId else timestampDataId}',
        | 'failing-source' = 'true'
        |)
        |""".stripMargin)
    tEnv.createFunction("concat_distinct_agg", classOf[ConcatDistinctAggFunction])

    tEnv.getConfig.setLocalTimeZone(SHANGHAI_ZONE)
    tEnv.getConfig.getConfiguration.setString(
      OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
      aggPhase.toString)
  }

  @Test
  def testEventTimeTumbleWindow(): Unit = {
    val sql =
      """
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
      "a,2020-10-10T00:00:05,2020-10-10T00:00:10,1,3.33,null,3.0,1,Comment#2",
      "b,2020-10-10T00:00:05,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:35,1,7.77,7.0,7.0,0,null")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeTumbleWindowWithOffset(): Unit = {
    val sql =
      """
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '1' DAY, INTERVAL '8' HOUR))
        |GROUP BY `name`, window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-09T08:00,2020-10-10T08:00,6,19.98,5.0,1.0,3,Hi|Comment#1|Comment#2",
      "b,2020-10-09T08:00,2020-10-10T08:00,4,14.43,6.0,3.0,3,Hello|Hi|Comment#3",
      "null,2020-10-09T08:00,2020-10-10T08:00,1,7.77,7.0,7.0,0,null")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testCascadeEventTimeTumbleWindowWithOffset(): Unit = {
    val sql =
      """
        |SELECT
        |  cnt,
        |  window_start,
        |  window_end,
        |  COUNT(*)
        |  FROM
        |  (
        |    SELECT
        |    `name`,
        |    window_start,
        |    window_end,
        |    COUNT(DISTINCT `string`) AS cnt
        |    FROM TABLE(
        |      TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '1' DAY, INTERVAL '8' HOUR))
        |    GROUP BY `name`, window_start, window_end
        |) GROUP BY cnt, window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "0,2020-10-09T08:00,2020-10-10T08:00,1",
      "3,2020-10-09T08:00,2020-10-10T08:00,2")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeTumbleWindowWithNegativeOffset(): Unit = {
    val sql =
      """
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '1' DAY, INTERVAL '-8' HOUR))
        |GROUP BY `name`, window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-09T16:00,2020-10-10T16:00,6,19.98,5.0,1.0,3,Hi|Comment#1|Comment#2",
      "b,2020-10-09T16:00,2020-10-10T16:00,4,14.43,6.0,3.0,3,Hello|Hi|Comment#3",
      "null,2020-10-09T16:00,2020-10-10T16:00,1,7.77,7.0,7.0,0,null")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeTumbleWindow_GroupingSets(): Unit = {
    val sql =
      """
        |SELECT
        |  GROUPING_ID(`name`),
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY GROUPING SETS((`name`),()), window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(
      TumbleWindowGroupSetExpectedData.sorted.mkString("\n"),
      sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeTumbleWindow_Cube(): Unit = {
    val sql =
      """
        |SELECT
        |  GROUPING_ID(`name`),
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY CUBE(`name`), window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(
      TumbleWindowCubeExpectedData.sorted.mkString("\n"),
      sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeTumbleWindow_Rollup(): Unit = {
    val sql =
      """
        |SELECT
        |  GROUPING_ID(`name`),
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY ROLLUP(`name`), window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(
      TumbleWindowRollupExpectedData.sorted.mkString("\n"),
      sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testTumbleWindowOutputWindowTime(): Unit = {
    val sql =
      """
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  window_time,
        |  COUNT(*)
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY `name`, window_start, window_end, window_time
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = if (useTimestampLtz) {
      Seq(
        "a,2020-10-10T00:00,2020-10-10T00:00:05,2020-10-09T16:00:04.999Z,4",
        "a,2020-10-10T00:00:05,2020-10-10T00:00:10,2020-10-09T16:00:09.999Z,1",
        "b,2020-10-10T00:00:05,2020-10-10T00:00:10,2020-10-09T16:00:09.999Z,2",
        "b,2020-10-10T00:00:15,2020-10-10T00:00:20,2020-10-09T16:00:19.999Z,1",
        "b,2020-10-10T00:00:30,2020-10-10T00:00:35,2020-10-09T16:00:34.999Z,1",
        "null,2020-10-10T00:00:30,2020-10-10T00:00:35,2020-10-09T16:00:34.999Z,1"
      )
    } else {
      Seq(
        "a,2020-10-10T00:00,2020-10-10T00:00:05,2020-10-10T00:00:04.999,4",
        "a,2020-10-10T00:00:05,2020-10-10T00:00:10,2020-10-10T00:00:09.999,1",
        "b,2020-10-10T00:00:05,2020-10-10T00:00:10,2020-10-10T00:00:09.999,2",
        "b,2020-10-10T00:00:15,2020-10-10T00:00:20,2020-10-10T00:00:19.999,1",
        "b,2020-10-10T00:00:30,2020-10-10T00:00:35,2020-10-10T00:00:34.999,1",
        "null,2020-10-10T00:00:30,2020-10-10T00:00:35,2020-10-10T00:00:34.999,1")
    }
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testTumbleWindowGroupOnWindowOnly(): Unit = {
    val sql =
      """
        |SELECT
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "2020-10-10T00:00,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
      "2020-10-10T00:00:05,2020-10-10T00:00:10,3,9.99,6.0,3.0,3,Hello|Hi|Comment#2",
      "2020-10-10T00:00:15,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
      "2020-10-10T00:00:30,2020-10-10T00:00:35,2,11.10,7.0,3.0,1,Comment#3")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testTumbleWindowWithoutOutputWindowColumns(): Unit = {
    val sql =
      """
        |SELECT
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   TUMBLE(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND))
        |GROUP BY window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "4,11.10,5.0,1.0,2,Hi|Comment#1",
      "3,9.99,6.0,3.0,3,Hello|Hi|Comment#2",
      "1,4.44,4.0,4.0,1,Hi",
      "2,11.10,7.0,3.0,1,Comment#3")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeHopWindow(): Unit = {
    val sql =
      """
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   HOP(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
        |GROUP BY `name`, window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-09T23:59:55,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
      "a,2020-10-10T00:00,2020-10-10T00:00:10,6,19.98,5.0,1.0,3,Comment#2|Hi|Comment#1",
      "a,2020-10-10T00:00:05,2020-10-10T00:00:15,1,3.33,null,3.0,1,Comment#2",
      "b,2020-10-10T00:00,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi",
      "b,2020-10-10T00:00:05,2020-10-10T00:00:15,2,6.66,6.0,3.0,2,Hello|Hi",
      "b,2020-10-10T00:00:10,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:25,1,4.44,4.0,4.0,1,Hi",
      "b,2020-10-10T00:00:25,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:40,1,3.33,3.0,3.0,1,Comment#3",
      "null,2020-10-10T00:00:25,2020-10-10T00:00:35,1,7.77,7.0,7.0,0,null",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:40,1,7.77,7.0,7.0,0,null")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeHopWindowWithOffset(): Unit = {
    val sql =
      """
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   HOP(
        |     TABLE T1,
        |     DESCRIPTOR(rowtime),
        |     INTERVAL '12' HOUR,
        |     INTERVAL '1' DAY,
        |     INTERVAL '8' HOUR))
        |GROUP BY `name`, window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-09T08:00,2020-10-10T08:00,6,19.98,5.0,1.0,3,Hi|Comment#1|Comment#2",
      "a,2020-10-09T20:00,2020-10-10T20:00,6,19.98,5.0,1.0,3,Hi|Comment#1|Comment#2",
      "b,2020-10-09T08:00,2020-10-10T08:00,4,14.43,6.0,3.0,3,Hello|Hi|Comment#3",
      "b,2020-10-09T20:00,2020-10-10T20:00,4,14.43,6.0,3.0,3,Hello|Hi|Comment#3",
      "null,2020-10-09T08:00,2020-10-10T08:00,1,7.77,7.0,7.0,0,null",
      "null,2020-10-09T20:00,2020-10-10T20:00,1,7.77,7.0,7.0,0,null")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeHopWindowWithNegativeOffset(): Unit = {
    val sql =
      """
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   HOP(
        |     TABLE T1,
        |     DESCRIPTOR(rowtime),
        |     INTERVAL '12' HOUR,
        |     INTERVAL '1' DAY,
        |     INTERVAL '-8' HOUR))
        |GROUP BY `name`, window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-09T04:00,2020-10-10T04:00,6,19.98,5.0,1.0,3,Hi|Comment#1|Comment#2",
      "a,2020-10-09T16:00,2020-10-10T16:00,6,19.98,5.0,1.0,3,Hi|Comment#1|Comment#2",
      "b,2020-10-09T04:00,2020-10-10T04:00,4,14.43,6.0,3.0,3,Hello|Hi|Comment#3",
      "b,2020-10-09T16:00,2020-10-10T16:00,4,14.43,6.0,3.0,3,Hello|Hi|Comment#3",
      "null,2020-10-09T04:00,2020-10-10T04:00,1,7.77,7.0,7.0,0,null",
      "null,2020-10-09T16:00,2020-10-10T16:00,1,7.77,7.0,7.0,0,null")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeHopWindow_GroupingSets(): Unit = {
    val sql =
      """
        |SELECT
        |  GROUPING_ID(`name`),
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   HOP(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
        |GROUP BY GROUPING SETS((`name`),()), window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(
      HopWindowGroupSetExpectedData.sorted.mkString("\n"),
      sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeHopWindow_Cube(): Unit = {
    val sql =
      """
        |SELECT
        |  GROUPING_ID(`name`),
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   HOP(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
        |GROUP BY CUBE(`name`), window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(
      HopWindowCubeExpectedData.sorted.mkString("\n"),
      sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeHopWindow_Rollup(): Unit = {
    val sql =
      """
        |SELECT
        |  GROUPING_ID(`name`),
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   HOP(TABLE T1, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND))
        |GROUP BY ROLLUP(`name`), window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(
      HopWindowRollupExpectedData.sorted.mkString("\n"),
      sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeCumulateWindow(): Unit = {
    val sql =
      """
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   CUMULATE(
        |     TABLE T1,
        |     DESCRIPTOR(rowtime),
        |     INTERVAL '5' SECOND,
        |     INTERVAL '15' SECOND))
        |GROUP BY `name`, window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-10T00:00,2020-10-10T00:00:05,4,11.10,5.0,1.0,2,Hi|Comment#1",
      "a,2020-10-10T00:00,2020-10-10T00:00:10,6,19.98,5.0,1.0,3,Hi|Comment#1|Comment#2",
      "a,2020-10-10T00:00,2020-10-10T00:00:15,6,19.98,5.0,1.0,3,Hi|Comment#1|Comment#2",
      "b,2020-10-10T00:00,2020-10-10T00:00:10,2,6.66,6.0,3.0,2,Hello|Hi",
      "b,2020-10-10T00:00,2020-10-10T00:00:15,2,6.66,6.0,3.0,2,Hello|Hi",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:20,1,4.44,4.0,4.0,1,Hi",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:25,1,4.44,4.0,4.0,1,Hi",
      "b,2020-10-10T00:00:15,2020-10-10T00:00:30,1,4.44,4.0,4.0,1,Hi",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:35,1,3.33,3.0,3.0,1,Comment#3",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:40,1,3.33,3.0,3.0,1,Comment#3",
      "b,2020-10-10T00:00:30,2020-10-10T00:00:45,1,3.33,3.0,3.0,1,Comment#3",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:35,1,7.77,7.0,7.0,0,null",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:40,1,7.77,7.0,7.0,0,null",
      "null,2020-10-10T00:00:30,2020-10-10T00:00:45,1,7.77,7.0,7.0,0,null")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeCumulateWindowWithOffset(): Unit = {
    val sql =
      """
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   CUMULATE(
        |     TABLE T1,
        |     DESCRIPTOR(rowtime),
        |     INTERVAL '12' HOUR,
        |     INTERVAL '1' DAY,
        |     INTERVAL '8' HOUR))
        |GROUP BY `name`, window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-09T08:00,2020-10-10T08:00,6,19.98,5.0,1.0,3,Hi|Comment#1|Comment#2",
      "b,2020-10-09T08:00,2020-10-10T08:00,4,14.43,6.0,3.0,3,Hello|Hi|Comment#3",
      "null,2020-10-09T08:00,2020-10-10T08:00,1,7.77,7.0,7.0,0,null")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeCumulateWindowWithNegativeOffset(): Unit = {
    val sql =
      """
        |SELECT
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   CUMULATE(
        |     TABLE T1,
        |     DESCRIPTOR(rowtime),
        |     INTERVAL '12' HOUR,
        |     INTERVAL '1' DAY,
        |     INTERVAL '-8' HOUR))
        |GROUP BY `name`, window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "a,2020-10-09T16:00,2020-10-10T04:00,6,19.98,5.0,1.0,3,Hi|Comment#1|Comment#2",
      "a,2020-10-09T16:00,2020-10-10T16:00,6,19.98,5.0,1.0,3,Hi|Comment#1|Comment#2",
      "b,2020-10-09T16:00,2020-10-10T04:00,4,14.43,6.0,3.0,3,Hello|Hi|Comment#3",
      "b,2020-10-09T16:00,2020-10-10T16:00,4,14.43,6.0,3.0,3,Hello|Hi|Comment#3",
      "null,2020-10-09T16:00,2020-10-10T04:00,1,7.77,7.0,7.0,0,null",
      "null,2020-10-09T16:00,2020-10-10T16:00,1,7.77,7.0,7.0,0,null")
    assertEquals(expected.sorted.mkString("\n"), sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeCumulateWindow_GroupingSets(): Unit = {
    val sql =
      """
        |SELECT
        |  GROUPING_ID(`name`),
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   CUMULATE(
        |     TABLE T1,
        |     DESCRIPTOR(rowtime),
        |     INTERVAL '5' SECOND,
        |     INTERVAL '15' SECOND))
        |GROUP BY GROUPING SETS((`name`),()), window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(
      CumulateWindowGroupSetExpectedData.sorted.mkString("\n"),
      sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeCumulateWindow_Cube(): Unit = {
    val sql =
      """
        |SELECT
        |  GROUPING_ID(`name`),
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   CUMULATE(
        |     TABLE T1,
        |     DESCRIPTOR(rowtime),
        |     INTERVAL '5' SECOND,
        |     INTERVAL '15' SECOND))
        |GROUP BY Cube(`name`), window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(
      CumulateWindowCubeExpectedData.sorted.mkString("\n"),
      sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeCumulateWindow_Rollup(): Unit = {
    val sql =
      """
        |SELECT
        |  GROUPING_ID(`name`),
        |  `name`,
        |  window_start,
        |  window_end,
        |  COUNT(*),
        |  SUM(`bigdec`),
        |  MAX(`double`),
        |  MIN(`float`),
        |  COUNT(DISTINCT `string`),
        |  concat_distinct_agg(`string`)
        |FROM TABLE(
        |   CUMULATE(
        |     TABLE T1,
        |     DESCRIPTOR(rowtime),
        |     INTERVAL '5' SECOND,
        |     INTERVAL '15' SECOND))
        |GROUP BY ROLLUP(`name`), window_start, window_end
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(
      CumulateWindowRollupExpectedData.sorted.mkString("\n"),
      sink.getAppendResults.sorted.mkString("\n"))
  }

  @Test
  def testEventTimeSessionWindow(): Unit = {
    //To verify the "merge" functionality, we create this test with the following characteristics:
    // 1. set the Parallelism to 1, and have the test data out of order
    // 2. create a waterMark with 10ms offset to delay the window emission by 10ms
    val sessionData = List(
      (1L, 1, "Hello", "a"),
      (2L, 2, "Hello", "b"),
      (8L, 8, "Hello", "a"),
      (9L, 9, "Hello World", "b"),
      (4L, 4, "Hello", "c"),
      (16L, 16, "Hello", "d"))

    val stream = failingDataSource(sessionData)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, String, String)](10L))
    val table = stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'string, 'name)
    tEnv.registerTable("T", table)

    val sql =
      """
        |SELECT
        |  `string`,
        |  window_start,
        |  window_time,
        |  COUNT(1),
        |  SUM(1),
        |  COUNT(`int`),
        |  SUM(`int`),
        |  COUNT(DISTINCT name)
        |FROM TABLE(
        |  SESSION(
        |    TABLE T,
        |    DESCRIPTOR(rowtime),
        |    DESCRIPTOR(`string`),
        |    INTERVAL '0.005' SECOND))
        |GROUP BY `string`, window_start, window_end, window_time
      """.stripMargin

    val sink = new TestingAppendSink
    tEnv.sqlQuery(sql).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "Hello World,1970-01-01T00:00:00.009,1970-01-01T00:00:00.013,1,1,1,9,1",
      "Hello,1970-01-01T00:00:00.016,1970-01-01T00:00:00.020,1,1,1,16,1",
      "Hello,1970-01-01T00:00:00.001,1970-01-01T00:00:00.012,4,4,4,15,3")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testDistinctAggWithMergeOnEventTimeSessionGroupWindow(): Unit = {
    // create a watermark with 10ms offset to delay the window emission by 10ms to verify merge
    val sessionWindowTestData = List(
      (1L, 2, "Hello"),       // (1, Hello)       - window
      (2L, 2, "Hello"),       // (1, Hello)       - window, deduped
      (8L, 2, "Hello"),       // (2, Hello)       - window, deduped during merge
      (10L, 3, "Hello"),      // (2, Hello)       - window, forwarded during merge
      (9L, 9, "Hello World"), // (1, Hello World) - window
      (4L, 1, "Hello"),       // (1, Hello)       - window, triggering merge
      (16L, 16, "Hello"))     // (3, Hello)       - window (not merged)

    val stream = failingDataSource(sessionWindowTestData)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset[(Long, Int, String)](10L))
    val table = stream.toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)
    tEnv.registerTable("MyTable", table)

    val sqlQuery =
      """
        |SELECT c,
        |   COUNT(DISTINCT b),
        |   window_end
        |FROM TABLE(
        |  SESSION(
        |    TABLE MyTable,
        |    DESCRIPTOR(rowtime),
        |    DESCRIPTOR(c),
        |    INTERVAL '0.005' SECOND))
        |GROUP BY c, window_start, window_end
      """.stripMargin
    val sink = new TestingAppendSink
    tEnv.sqlQuery(sqlQuery).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = Seq(
      "Hello World,1,1970-01-01T00:00:00.014", // window starts at [9L] till {14L}
      "Hello,1,1970-01-01T00:00:00.021",       // window starts at [16L] till {21L}, not merged
      "Hello,3,1970-01-01T00:00:00.015"        // window starts at [1L,2L],
      //   merged with [8L,10L], by [4L], till {15L}
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }
}

object WindowAggregateITCase {

  @Parameterized.Parameters(name = "AggPhase={0}, StateBackend={1}, UseTimestampLtz = {2}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    Seq[Array[AnyRef]](
      // we do not test all cases to simplify the test matrix
      Array(ONE_PHASE, HEAP_BACKEND, java.lang.Boolean.TRUE),
      Array(TWO_PHASE, HEAP_BACKEND, java.lang.Boolean.FALSE),
      Array(ONE_PHASE, ROCKSDB_BACKEND, java.lang.Boolean.FALSE),
      Array(TWO_PHASE, ROCKSDB_BACKEND, java.lang.Boolean.TRUE))
  }
}
