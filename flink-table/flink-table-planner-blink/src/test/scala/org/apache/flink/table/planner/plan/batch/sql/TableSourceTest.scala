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

package org.apache.flink.table.planner.plan.batch.sql

import org.apache.flink.table.planner.expressions.utils.Func1
import org.apache.flink.table.planner.utils._
import org.junit.{Before, Test}

class TableSourceTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE ProjectableTable (
         |  a int,
         |  b bigint,
         |  c varchar(32)
         |) WITH (
         |  'connector' = 'values',
         |  'bounded' = 'true'
         |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)
    val ddl2 =
      """
        |CREATE TABLE NestedTable (
        |  id int,
        |  deepNested row<nested1 row<name string, `value` int>, nested2 row<num int, flag
        |  boolean>>,
        |  nested row<name string, `value` int>,
        |  name string
        |) WITH (
        | 'connector' = 'values',
        |  'bounded' = 'true'
        |)
        |""".stripMargin
    util.tableEnv.executeSql(ddl2)
    val ddl3 =
      """
        |CREATE TABLE PartitionableTable (
        |  id int,
        |  name string,
        |  part1 string,
        |  part2 int
        |) PARTITIONED BY (`part1`,`part2`)
        |WITH (
        |  'connector' = 'values',
        |  'bounded' = 'true',
        |  'use-partition-push-down' = 'true',
        |  'partition-list' = 'part1:A, part2:1;part1:A, part2:2;part1:B, part2:3;part1:C, part2:1'
        |)
        |""".stripMargin
    util.tableEnv.executeSql(ddl3)
  }

  @Test
  def testSimpleProject(): Unit = {
    util.verifyPlan("SELECT a, c FROM ProjectableTable")
  }

  @Test
  def testProjectWithoutInputRef(): Unit = {
    util.verifyPlan("SELECT COUNT(1) FROM ProjectableTable")
  }

  @Test
  def testNestedProject(): Unit = {
    val sqlQuery =
      """
        |SELECT id,
        |    deepNested.nested1.name AS nestedName,
        |    nested.`value` AS nestedValue,
        |    deepNested.nested2.flag AS nestedFlag,
        |    deepNested.nested2.num AS nestedNum
        |FROM NestedTable
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPartitionTableSource(): Unit = {
    util.verifyPlan("SELECT * FROM PartitionableTable WHERE part2 > 1 and id > 2 AND part1 = 'A' ")
  }

  @Test
  def testPartitionTableSourceWithUdf(): Unit = {
    util.addFunction("MyUdf", Func1)
    util.verifyPlan("SELECT * FROM PartitionableTable WHERE id > 2 AND MyUdf(part2) < 3")
  }
}
