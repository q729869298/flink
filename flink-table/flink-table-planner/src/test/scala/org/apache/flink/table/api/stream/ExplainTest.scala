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

package org.apache.flink.table.api.stream

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.utils.MemoryTableSourceSinkUtil
import org.apache.flink.table.utils.TableTestUtil.{readFromResource, replaceStageId, streamTableNode}
import org.apache.flink.test.util.AbstractTestBase

import org.junit.Assert.assertEquals
import org.junit._

class ExplainTest extends AbstractTestBase {

  @Test
  def testFilter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val scan = env.fromElements((1, "hello")).toTable(tEnv, 'a, 'b)
    val table = scan.filter($"a" % 2 === 0)

    val result = replaceStageId(tEnv.explain(table))

    val source = readFromResource("testFilterStream0.out")
    val expect = replaceString(source, scan)
    assertEquals(expect, result)
  }

  @Test
  def testUnion(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val table1 = env.fromElements((1, "hello")).toTable(tEnv, 'count, 'word)
    val table2 = env.fromElements((1, "hello")).toTable(tEnv, 'count, 'word)
    val table = table1.unionAll(table2)

    val result = replaceStageId(tEnv.explain(table))

    val source = readFromResource("testUnionStream0.out")
    val expect = replaceString(source, table1, table2)
    assertEquals(expect, result)
  }

  @Test
  def testInsert(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "sourceTable", CommonTestData.getCsvTableSource)

    val fieldNames = Array("d", "e")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING(), Types.INT())
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable", sink.configure(fieldNames, fieldTypes))

    tEnv.sqlUpdate("INSERT INTO targetTable SELECT first, id FROM sourceTable")

    val result = tEnv.explain(false)
    val source = readFromResource("testInsert.out")
    assertEquals(replaceStageId(source), replaceStageId(result))
  }

  @Test
  def testMultipleInserts(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "sourceTable", CommonTestData.getCsvTableSource)

    val fieldNames = Array("d", "e")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING(), Types.INT())
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable1", sink.configure(fieldNames, fieldTypes))
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable2", sink.configure(fieldNames, fieldTypes))

    tEnv.sqlUpdate("INSERT INTO targetTable1 SELECT first, id FROM sourceTable")
    tEnv.sqlUpdate("INSERT INTO targetTable2 SELECT last, id FROM sourceTable")

    val result = tEnv.explain(false)
    val source = readFromResource("testMultipleInserts.out")
    assertEquals(replaceStageId(source), replaceStageId(result))
  }

  @Test
  def testStreamTableEnvironmentExecutionExplain(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "sourceTable", CommonTestData.getCsvTableSource)

    val fieldNames = Array("d", "e")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING(), Types.INT())
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable", sink.configure(fieldNames, fieldTypes))

    val actual = tEnv.explainSql("INSERT INTO targetTable SELECT first, id FROM sourceTable",
      ExplainDetail.JSON_EXECUTION_PLAN)
    val expected = readFromResource("testStreamTableEnvironmentExecutionExplain.out")

    assertEquals(replaceStreamNodeId(expected), replaceStreamNodeId(actual))
  }

  @Test
  def testStatementSetExecutionExplain(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "sourceTable", CommonTestData.getCsvTableSource)

    val fieldNames = Array("d", "e")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING(), Types.INT())
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal(
      "targetTable", sink.configure(fieldNames, fieldTypes))

    val statementSet = tEnv.createStatementSet()
    statementSet.addInsertSql("INSERT INTO targetTable SELECT first, id FROM sourceTable")

    val actual = statementSet.explain(ExplainDetail.JSON_EXECUTION_PLAN)
    val expected = readFromResource("testStatementSetExecutionExplain0.out")

    assertEquals(replaceStreamNodeId(expected), replaceStreamNodeId(actual))
  }

  def replaceString(s: String, t1: Table, t2: Table): String = {
    replaceSourceNode(replaceSourceNode(replaceStageId(s), t1, 0), t2, 1)
  }

  def replaceString(s: String, t: Table): String = {
    replaceSourceNode(replaceStageId(s), t, 0)
  }

  private def replaceSourceNode(s: String, t: Table, idx: Int): String = {
    replaceStageId(s)
      .replace(
        s"%logicalSourceNode$idx%", streamTableNode(t)
          .replace("DataStreamScan", "FlinkLogicalDataStreamScan"))
      .replace(s"%sourceNode$idx%", streamTableNode(t))
  }

  def replaceStreamNodeId(s: String): String = {
    s.replaceAll("\"id\" : \\d+", "\"id\" : ").trim
  }
}
