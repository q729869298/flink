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
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.table.api.scala.stream.sql.SqlITCase.EventTimeSourceFunction
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
      Right(1L),
      Left((2L, (2L, 2, "Hello"))),
      Right(2L),
      Left((3L, (3L, 3, "Hello"))),
      Right(3L),
      Left((4L, (4L, 4, "Hello"))),
      Right(4L),
      Left((5L, (5L, 5, "Hello"))),
      Right(5L),
      Left((6L, (6L, 6, "Hello"))),
      Right(6L),
      Left((7L, (7L, 7, "Hello World"))),
      Right(7L),
      Left((8L, (8L, 8, "Hello World"))),
      Right(8L),
      Left((20L, (20L, 20, "Hello World"))),
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
      "Hello,1,1,1", "Hello,2,2,3", "Hello,3,3,6", "Hello,4,3,9", "Hello,5,3,12",
      "Hello,6,3,15", "Hello World,7,1,7", "Hello World,8,2,15", "Hello World,20,3,35")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testBoundNonPartitionedEventTimeWindowWithRow(): Unit = {

    val data = Seq(
      Left((1L, (1L, 1, "Hello"))),
      Right(1L),
      Left((2L, (2L, 2, "Hello"))),
      Right(2L),
      Left((3L, (3L, 3, "Hello"))),
      Right(3L),
      Left((4L, (4L, 4, "Hello"))),
      Right(4L),
      Left((5L, (5L, 5, "Hello"))),
      Right(5L),
      Left((6L, (6L, 6, "Hello"))),
      Right(6L),
      Left((7L, (7L, 7, "Hello World"))),
      Right(7L),
      Left((8L, (8L, 8, "Hello World"))),
      Right(8L),
      Left((20L, (20L, 20, "Hello World"))),
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
      "count(a) OVER (ORDER BY RowTime() ROWS BETWEEN 2 preceding AND CURRENT ROW)," +
      "sum(a) OVER (ORDER BY RowTime() ROWS BETWEEN 2 preceding AND CURRENT ROW)" +
      "from T1"

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "Hello,1,1,1", "Hello,2,2,3", "Hello,3,3,6", "Hello,4,3,9", "Hello,5,3,12",
      "Hello,6,3,15", "Hello World,7,3,18", "Hello World,8,3,21", "Hello World,20,3,35")
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

}

object SqlITCase {

  class EventTimeSourceFunction[T](
      dataWithTimestampList: Seq[Either[(Long, T), Long]]) extends SourceFunction[T] {
    override def run(ctx: SourceContext[T]): Unit = {
      dataWithTimestampList.foreach { x =>
        if (x.isLeft) {
          ctx.collectWithTimestamp(x.left.get._2, x.left.get._1)
        } else if (x.isRight) {
          ctx.emitWatermark(new Watermark(x.right.get))
        }
      }
    }

    override def cancel(): Unit = ???
  }

}
