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

package org.apache.flink.table.api.scala.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala.stream.sql.SqlITCase.{
  EventTimeSourceFunction, 
  RowResultSortComparator,
  TupleRowSelector}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.utils.{StreamITCase, StreamTestData, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import scala.collection.mutable
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness
import org.apache.flink.streaming.util.TestHarnessUtil
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.table.runtime.aggregate.ProcTimeBoundedProcessingOverProcessFunction
import org.apache.flink.table.functions.aggfunctions.CountAggFunction
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.typeutils.ValueTypeInfo._
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.Comparator
import java.io.ObjectInputStream.GetField
import org.apache.flink.api.common.typeinfo._

class SqlITCase extends StreamingWithStateTestBase {

  val data = List(
    (1L, 1, "Hello"),
    (2L, 2, "Hello"),
    (3L, 3, "Hello"),
    (4L, 4, "Hello"),
    (5L, 5, "Hello"),
    (6L, 6, "Hello"),
    (7L, 7, "Hello World"),
    (8L, 8, "Hello World"),
    (20L, 20, "Hello World"))

  /** test selection **/
  @Test
  def testSelectExpressionFromTable(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a * 2, b - 1 FROM MyTable"

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("2,0", "4,1", "6,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test filtering with registered table **/
  @Test
  def testSimpleFilter(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT * FROM MyTable WHERE a = 3"

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test filtering with registered datastream **/
  @Test
  def testDatastreamFilter(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT * FROM MyTable WHERE _1 = 3"

    val t = StreamTestData.getSmall3TupleDataStream(env)
    tEnv.registerDataStream("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test union with registered tables **/
  @Test
  def testUnion(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT * FROM T1 " +
      "UNION ALL " +
      "SELECT * FROM T2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T2", t2)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,Hi", "1,1,Hi",
      "2,2,Hello", "2,2,Hello",
      "3,2,Hello world", "3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test union with filter **/
  @Test
  def testUnionWithFilter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT * FROM T1 WHERE a = 3 " +
      "UNION ALL " +
      "SELECT * FROM T2 WHERE a = 2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T2", t2)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "2,2,Hello",
      "3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test union of a table and a datastream **/
  @Test
  def testUnionTableWithDataSet(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT c FROM T1 WHERE a = 3 " +
      "UNION ALL " +
      "SELECT c FROM T2 WHERE a = 2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.get3TupleDataStream(env)
    tEnv.registerDataStream("T2", t2, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("Hello", "Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnboundPartitionedProcessingWindowWithRange(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    // for sum aggregation ensure that every time the order of each element is consistent
    env.setParallelism(1)

    val t1 = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY ProcTime()  RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (PARTITION BY c ORDER BY ProcTime() RANGE UNBOUNDED preceding) as cnt2 " +
      "from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "Hello World,1,7", "Hello World,2,15", "Hello World,3,35",
      "Hello,1,1", "Hello,2,3", "Hello,3,6", "Hello,4,10", "Hello,5,15", "Hello,6,21")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnboundPartitionedProcessingWindowWithRow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val t1 = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY ProcTime() ROWS BETWEEN UNBOUNDED preceding AND " +
      "CURRENT ROW)" +
      "from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "Hello World,1", "Hello World,2", "Hello World,3",
      "Hello,1", "Hello,2", "Hello,3", "Hello,4", "Hello,5", "Hello,6")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnboundNonPartitionedProcessingWindowWithRange(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    // for sum aggregation ensure that every time the order of each element is consistent
    env.setParallelism(1)

    val t1 = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, " +
      "count(a) OVER (ORDER BY ProcTime()  RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (ORDER BY ProcTime() RANGE UNBOUNDED preceding) as cnt2 " +
      "from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "Hello World,7,28", "Hello World,8,36", "Hello World,9,56",
      "Hello,1,1", "Hello,2,3", "Hello,3,6", "Hello,4,10", "Hello,5,15", "Hello,6,21")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUnboundNonPartitionedProcessingWindowWithRow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val t1 = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "count(a) OVER (ORDER BY ProcTime() ROWS BETWEEN UNBOUNDED preceding AND CURRENT ROW)" +
      "from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("1", "2", "3", "4", "5", "6", "7", "8", "9")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testBoundPartitionedEventTimeWindowWithRow(): Unit = {
    val data = Seq(
      Left((1L, (1L, 1, "Hello"))),
      Left((2L, (2L, 2, "Hello"))),
      Left((1L, (1L, 1, "Hello"))),
      Left((2L, (2L, 2, "Hello"))),
      Left((2L, (2L, 2, "Hello"))),
      Left((1L, (1L, 1, "Hello"))),
      Left((3L, (7L, 7, "Hello World"))),
      Left((1L, (7L, 7, "Hello World"))),
      Left((1L, (7L, 7, "Hello World"))),
      Right(2L),
      Left((3L, (3L, 3, "Hello"))),
      Left((4L, (4L, 4, "Hello"))),
      Left((5L, (5L, 5, "Hello"))),
      Left((6L, (6L, 6, "Hello"))),
      Left((20L, (20L, 20, "Hello World"))),
      Right(6L),
      Left((8L, (8L, 8, "Hello World"))),
      Left((7L, (7L, 7, "Hello World"))),
      Right(20L))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t1 = env
      .addSource[(Long, Int, String)](new EventTimeSourceFunction[(Long, Int, String)](data))
      .toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, a, " +
      "count(a) OVER (PARTITION BY c ORDER BY RowTime() ROWS BETWEEN 2 preceding AND CURRENT ROW)" +
      ", sum(a) OVER (PARTITION BY c ORDER BY RowTime() ROWS BETWEEN 2 preceding AND CURRENT ROW)" +
      " from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "Hello,1,1,1", "Hello,1,2,2", "Hello,1,3,3",
      "Hello,2,3,4", "Hello,2,3,5","Hello,2,3,6",
      "Hello,3,3,7", "Hello,4,3,9", "Hello,5,3,12",
      "Hello,6,3,15",
      "Hello World,7,1,7", "Hello World,7,2,14", "Hello World,7,3,21",
      "Hello World,7,3,21", "Hello World,8,3,22", "Hello World,20,3,35")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testBoundNonPartitionedEventTimeWindowWithRow(): Unit = {

    val data = Seq(
      Left((2L, (2L, 2, "Hello"))),
      Left((2L, (2L, 2, "Hello"))),
      Left((1L, (1L, 1, "Hello"))),
      Left((1L, (1L, 1, "Hello"))),
      Left((2L, (2L, 2, "Hello"))),
      Left((1L, (1L, 1, "Hello"))),
      Left((20L, (20L, 20, "Hello World"))), // early row
      Right(3L),
      Left((2L, (2L, 2, "Hello"))), // late row
      Left((3L, (3L, 3, "Hello"))),
      Left((4L, (4L, 4, "Hello"))),
      Left((5L, (5L, 5, "Hello"))),
      Left((6L, (6L, 6, "Hello"))),
      Left((7L, (7L, 7, "Hello World"))),
      Right(7L),
      Left((9L, (9L, 9, "Hello World"))),
      Left((8L, (8L, 8, "Hello World"))),
      Left((8L, (8L, 8, "Hello World"))),
      Right(20L))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t1 = env
      .addSource[(Long, Int, String)](new EventTimeSourceFunction[(Long, Int, String)](data))
      .toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, a, " +
      "count(a) OVER (ORDER BY RowTime() ROWS BETWEEN 2 preceding AND CURRENT ROW)," +
      "sum(a) OVER (ORDER BY RowTime() ROWS BETWEEN 2 preceding AND CURRENT ROW)" +
      "from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "Hello,1,1,1", "Hello,1,2,2", "Hello,1,3,3",
      "Hello,2,3,4", "Hello,2,3,5", "Hello,2,3,6",
      "Hello,3,3,7",
      "Hello,4,3,9", "Hello,5,3,12",
      "Hello,6,3,15", "Hello World,7,3,18",
      "Hello World,8,3,21", "Hello World,8,3,23",
      "Hello World,9,3,25",
      "Hello World,20,3,37")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /**
    *  All aggregates must be computed on the same window.
    */
  @Test(expected = classOf[TableException])
  def testMultiWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val t1 = env.fromCollection(data).toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val sqlQuery = "SELECT " +
      "c, " +
      "count(a) OVER (PARTITION BY c ORDER BY ProcTime() RANGE UNBOUNDED preceding) as cnt1, " +
      "sum(a) OVER (PARTITION BY b ORDER BY ProcTime() RANGE UNBOUNDED preceding) as cnt2 " +
      "from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()
  }

  /** test sliding event-time unbounded window with partition by **/
  @Test
  def testUnboundedEventTimeRowWindowWithPartition(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    StreamITCase.testResults = mutable.MutableList()
    env.setParallelism(1)

    val sqlQuery = "SELECT a, b, c, " +
      "SUM(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row), " +
      "count(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row), " +
      "avg(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row), " +
      "max(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row), " +
      "min(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row) " +
      "from T1"

    val data = Seq(
      Left(14000005L, (1, 1L, "Hi")),
      Left(14000000L, (2, 1L, "Hello")),
      Left(14000002L, (3, 1L, "Hello")),
      Left(14000003L, (1, 2L, "Hello")),
      Left(14000004L, (1, 3L, "Hello world")),
      Left(14000007L, (3, 2L, "Hello world")),
      Left(14000008L, (2, 2L, "Hello world")),
      Right(14000010L),
      // the next 3 elements are late
      Left(14000008L, (1, 4L, "Hello world")),
      Left(14000008L, (2, 3L, "Hello world")),
      Left(14000008L, (3, 3L, "Hello world")),
      Left(14000012L, (1, 5L, "Hello world")),
      Right(14000020L),
      Left(14000021L, (1, 6L, "Hello world")),
      // the next 3 elements are late
      Left(14000019L, (1, 6L, "Hello world")),
      Left(14000018L, (2, 4L, "Hello world")),
      Left(14000018L, (3, 4L, "Hello world")),
      Left(14000022L, (2, 5L, "Hello world")),
      Left(14000022L, (3, 5L, "Hello world")),
      Left(14000024L, (1, 7L, "Hello world")),
      Left(14000023L, (1, 8L, "Hello world")),
      Left(14000021L, (1, 9L, "Hello world")),
      Right(14000030L)
    )

    val t1 = env.addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,2,Hello,2,1,2,2,2",
      "1,3,Hello world,5,2,2,3,2",
      "1,1,Hi,6,3,2,3,1",
      "2,1,Hello,1,1,1,1,1",
      "2,2,Hello world,3,2,1,2,1",
      "3,1,Hello,1,1,1,1,1",
      "3,2,Hello world,3,2,1,2,1",
      "1,5,Hello world,11,4,2,5,1",
      "1,6,Hello world,17,5,3,6,1",
      "1,9,Hello world,26,6,4,9,1",
      "1,8,Hello world,34,7,4,9,1",
      "1,7,Hello world,41,8,5,9,1",
      "2,5,Hello world,8,3,2,5,1",
      "3,5,Hello world,8,3,2,5,1"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test sliding event-time unbounded window with partition by **/
  @Test
  def testUnboundedEventTimeRowWindowWithPartitionMultiThread(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, b, c, " +
      "SUM(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row), " +
      "count(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row), " +
      "avg(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row), " +
      "max(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row), " +
      "min(b) over (" +
      "partition by a order by rowtime() rows between unbounded preceding and current row) " +
      "from T1"

    val data = Seq(
      Left(14000005L, (1, 1L, "Hi")),
      Left(14000000L, (2, 1L, "Hello")),
      Left(14000002L, (3, 1L, "Hello")),
      Left(14000003L, (1, 2L, "Hello")),
      Left(14000004L, (1, 3L, "Hello world")),
      Left(14000007L, (3, 2L, "Hello world")),
      Left(14000008L, (2, 2L, "Hello world")),
      Right(14000010L),
      Left(14000012L, (1, 5L, "Hello world")),
      Right(14000020L),
      Left(14000021L, (1, 6L, "Hello world")),
      Left(14000023L, (2, 5L, "Hello world")),
      Left(14000024L, (3, 5L, "Hello world")),
      Left(14000026L, (1, 7L, "Hello world")),
      Left(14000025L, (1, 8L, "Hello world")),
      Left(14000022L, (1, 9L, "Hello world")),
      Right(14000030L)
    )

    val t1 = env.addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,2,Hello,2,1,2,2,2",
      "1,3,Hello world,5,2,2,3,2",
      "1,1,Hi,6,3,2,3,1",
      "2,1,Hello,1,1,1,1,1",
      "2,2,Hello world,3,2,1,2,1",
      "3,1,Hello,1,1,1,1,1",
      "3,2,Hello world,3,2,1,2,1",
      "1,5,Hello world,11,4,2,5,1",
      "1,6,Hello world,17,5,3,6,1",
      "1,9,Hello world,26,6,4,9,1",
      "1,8,Hello world,34,7,4,9,1",
      "1,7,Hello world,41,8,5,9,1",
      "2,5,Hello world,8,3,2,5,1",
      "3,5,Hello world,8,3,2,5,1"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test sliding event-time unbounded window without partitiion by **/
  @Test
  def testUnboundedEventTimeRowWindowWithoutPartition(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    StreamITCase.testResults = mutable.MutableList()
    env.setParallelism(1)

    val sqlQuery = "SELECT a, b, c, " +
      "SUM(b) over (order by rowtime() rows between unbounded preceding and current row), " +
      "count(b) over (order by rowtime() rows between unbounded preceding and current row), " +
      "avg(b) over (order by rowtime() rows between unbounded preceding and current row), " +
      "max(b) over (order by rowtime() rows between unbounded preceding and current row), " +
      "min(b) over (order by rowtime() rows between unbounded preceding and current row) " +
      "from T1"

    val data = Seq(
      Left(14000005L, (1, 1L, "Hi")),
      Left(14000000L, (2, 2L, "Hello")),
      Left(14000002L, (3, 5L, "Hello")),
      Left(14000003L, (1, 3L, "Hello")),
      Left(14000004L, (3, 7L, "Hello world")),
      Left(14000007L, (4, 9L, "Hello world")),
      Left(14000008L, (5, 8L, "Hello world")),
      Right(14000010L),
      // this element will be discard because it is late
      Left(14000008L, (6, 8L, "Hello world")),
      Right(14000020L),
      Left(14000021L, (6, 8L, "Hello world")),
      Right(14000030L)
    )

    val t1 = env.addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "2,2,Hello,2,1,2,2,2",
      "3,5,Hello,7,2,3,5,2",
      "1,3,Hello,10,3,3,5,2",
      "3,7,Hello world,17,4,4,7,2",
      "1,1,Hi,18,5,3,7,1",
      "4,9,Hello world,27,6,4,9,1",
      "5,8,Hello world,35,7,5,9,1",
      "6,8,Hello world,43,8,5,9,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test sliding event-time unbounded window without partitiion by and arrive early **/
  @Test
  def testUnboundedEventTimeRowWindowArriveEarly(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    StreamITCase.testResults = mutable.MutableList()
    env.setParallelism(1)

    val sqlQuery = "SELECT a, b, c, " +
      "SUM(b) over (order by rowtime() rows between unbounded preceding and current row), " +
      "count(b) over (order by rowtime() rows between unbounded preceding and current row), " +
      "avg(b) over (order by rowtime() rows between unbounded preceding and current row), " +
      "max(b) over (order by rowtime() rows between unbounded preceding and current row), " +
      "min(b) over (order by rowtime() rows between unbounded preceding and current row) " +
      "from T1"

    val data = Seq(
      Left(14000005L, (1, 1L, "Hi")),
      Left(14000000L, (2, 2L, "Hello")),
      Left(14000002L, (3, 5L, "Hello")),
      Left(14000003L, (1, 3L, "Hello")),
      // next three elements are early
      Left(14000012L, (3, 7L, "Hello world")),
      Left(14000013L, (4, 9L, "Hello world")),
      Left(14000014L, (5, 8L, "Hello world")),
      Right(14000010L),
      Left(14000011L, (6, 8L, "Hello world")),
      // next element is early
      Left(14000021L, (6, 8L, "Hello world")),
      Right(14000020L),
      Right(14000030L)
    )

    val t1 = env.addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv).as('a, 'b, 'c)

    tEnv.registerTable("T1", t1)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "2,2,Hello,2,1,2,2,2",
      "3,5,Hello,7,2,3,5,2",
      "1,3,Hello,10,3,3,5,2",
      "1,1,Hi,11,4,2,5,1",
      "6,8,Hello world,19,5,3,8,1",
      "3,7,Hello world,26,6,4,8,1",
      "4,9,Hello world,35,7,5,9,1",
      "5,8,Hello world,43,8,5,9,1",
      "6,8,Hello world,51,9,5,9,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAvgSumAggregatationPartition(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, AVG(c) OVER (PARTITION BY a ORDER BY procTime()" +
      "RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS avgC," +
      "SUM(c) OVER (PARTITION BY a ORDER BY procTime()" +
      "RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) as sumC FROM MyTable"

    val t = StreamTestData.get5TupleDataStream(env)
      .toTable(tEnv).as('a, 'b, 'c, 'd, 'e)

    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,0,0",
      "2,1,1",
      "2,1,3",
      "3,3,3",
      "3,3,7",
      "3,4,12",
      "4,6,13",
      "4,6,6",
      "4,7,21",
      "4,7,30",
      "5,10,10",
      "5,10,21",
      "5,11,33",
      "5,11,46",
      "5,12,60")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAvgSumAggregatationNonPartition(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a, Count(c) OVER (ORDER BY procTime()" +
      "RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS avgC," +
      "MIN(c) OVER (ORDER BY procTime()" +
      "RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) as sumC FROM MyTable"

    val t = StreamTestData.get5TupleDataStream(env)
      .toTable(tEnv).as('a, 'b, 'c, 'd, 'e)

    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,0",
      "2,2,0",
      "2,3,0",
      "3,4,0",
      "3,5,0",
      "3,6,0",
      "4,7,0",
      "4,8,0",
      "4,9,0",
      "4,10,0",
      "5,11,0",
      "5,12,0",
      "5,13,0",
      "5,14,0",
      "5,15,0")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  
  @Test
  def testCountAggregatationProcTimeHarnessPartitioned(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    
    val rT =  new RowTypeInfo(Array[TypeInformation[_]](
      INT_TYPE_INFO,
      LONG_TYPE_INFO,
      INT_TYPE_INFO,
      STRING_TYPE_INFO,
      LONG_TYPE_INFO),
      Array("a","b","c","d","e"))
    
    val rTA =  new RowTypeInfo(Array[TypeInformation[_]](
     LONG_TYPE_INFO), Array("count"))
      
    val processFunction = new KeyedProcessOperator[String,Row,Row](
      new ProcTimeBoundedProcessingOverProcessFunction(
        Array(new CountAggFunction),
        Array(1),
        5,
        rTA,
        1000,
        rT))
  
    val rInput:Row = new Row(5)
      rInput.setField(0, 1)
      rInput.setField(1, 11L)
      rInput.setField(2, 1)
      rInput.setField(3, "aaa")
      rInput.setField(4, 11L)
    
   val testHarness = new KeyedOneInputStreamOperatorTestHarness[String,Row,Row](
      processFunction, 
      new TupleRowSelector(3), 
      BasicTypeInfo.STRING_TYPE_INFO)
    
   testHarness.open();

   testHarness.setProcessingTime(3)

   // timestamp is ignored in processing time
    testHarness.processElement(new StreamRecord(rInput, 1001))
    testHarness.processElement(new StreamRecord(rInput, 2002))
    testHarness.processElement(new StreamRecord(rInput, 2003))
    testHarness.processElement(new StreamRecord(rInput, 2004))
   
    testHarness.setProcessingTime(1004)
  
    testHarness.processElement(new StreamRecord(rInput, 2005))
    testHarness.processElement(new StreamRecord(rInput, 2006))
  
    val result = testHarness.getOutput
    
    val expectedOutput = new ConcurrentLinkedQueue[Object]()
    
     val rOutput:Row = new Row(6)
      rOutput.setField(0, 1)
      rOutput.setField(1, 11L)
      rOutput.setField(2, 1)
      rOutput.setField(3, "aaa")
      rOutput.setField(4, 11L)
      rOutput.setField(5, 1L)   //count is 1
    expectedOutput.add(new StreamRecord(rOutput, 1001));
    val rOutput2:Row = new Row(6)
      rOutput2.setField(0, 1)
      rOutput2.setField(1, 11L)
      rOutput2.setField(2, 1)
      rOutput2.setField(3, "aaa")
      rOutput2.setField(4, 11L)
      rOutput2.setField(5, 2L)   //count is 2 
    expectedOutput.add(new StreamRecord(rOutput2, 2002));
    val rOutput3:Row = new Row(6)
      rOutput3.setField(0, 1)
      rOutput3.setField(1, 11L)
      rOutput3.setField(2, 1)
      rOutput3.setField(3, "aaa")
      rOutput3.setField(4, 11L)
      rOutput3.setField(5, 3L)   //count is 3
    expectedOutput.add(new StreamRecord(rOutput3, 2003));
    val rOutput4:Row = new Row(6)
      rOutput4.setField(0, 1)
      rOutput4.setField(1, 11L)
      rOutput4.setField(2, 1)
      rOutput4.setField(3, "aaa")
      rOutput4.setField(4, 11L)
      rOutput4.setField(5, 4L)   //count is 4 
    expectedOutput.add(new StreamRecord(rOutput4, 2004));
    val rOutput5:Row = new Row(6)
      rOutput5.setField(0, 1)
      rOutput5.setField(1, 11L)
      rOutput5.setField(2, 1)
      rOutput5.setField(3, "aaa")
      rOutput5.setField(4, 11L)
      rOutput5.setField(5, 1L)   //count is reset to 1 
    expectedOutput.add(new StreamRecord(rOutput5, 2005));
    val rOutput6:Row = new Row(6)
      rOutput6.setField(0, 1)
      rOutput6.setField(1, 11L)
      rOutput6.setField(2, 1)
      rOutput6.setField(3, "aaa")
      rOutput6.setField(4, 11L)
      rOutput6.setField(5, 2L)   //count is 2 
    expectedOutput.add(new StreamRecord(rOutput6, 2006));
    
    TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.",
        expectedOutput, testHarness.getOutput(), new RowResultSortComparator(6))
    
    testHarness.close()
        
  }
  
}

object SqlITCase {

  class EventTimeSourceFunction[T](
      dataWithTimestampList: Seq[Either[(Long, T), Long]]) extends SourceFunction[T] {
    override def run(ctx: SourceContext[T]): Unit = {
      dataWithTimestampList.foreach {
        case Left(t) => ctx.collectWithTimestamp(t._2, t._1)
        case Right(w) => ctx.emitWatermark(new Watermark(w))
      }
    }

    override def cancel(): Unit = ???
  }
  

/*
 * Return 0 for equal Rows and non zero for different rows
 */

class RowResultSortComparator(
    private val IndexCounter:Int) extends Comparator[Object] with Serializable {
  
    override def compare( o1:Object, o2:Object):Int = {
  
      if (o1.isInstanceOf[Watermark] || o2.isInstanceOf[Watermark]) {
         0 
       } else {
        val sr0 = o1.asInstanceOf[StreamRecord[Row]]
        val sr1 = o2.asInstanceOf[StreamRecord[Row]]
        val row0 = sr0.getValue
        val row1 = sr1.getValue
        if ( row0.getArity != row1.getArity) {
          -1
        }
  
        if (sr0.getTimestamp != sr1.getTimestamp) {
          (sr0.getTimestamp() - sr1.getTimestamp())
        }

        var i = 0
        var result:Boolean = true
        while (i < IndexCounter) {
          result = (result) & (row0.getField(i)!=row1.getField(i))
          i += 1
        }
  
        if (result) {
          0
        } else {
          -1
        }
      }
   }
}

/*
 * Simple test class that returns a specified field as the selector function
 */
class TupleRowSelector(
    private val selectorField:Int) extends KeySelector[Row, String] {

  override def getKey(value: Row): String = { 
    value.getField(selectorField).asInstanceOf[String]
  }
}

}



