package org.apache.flink.table.runtime.batch.sql

import org.apache.flink.table.api.TableException
import org.apache.flink.table.runtime.utils.BatchTestBase
import org.apache.flink.table.runtime.utils.BatchTestBase.row
import org.apache.flink.table.runtime.utils.TestData.numericType

import org.junit.{Before, Test}

import java.math.{BigDecimal => JBigDecimal}

import scala.collection.Seq


class Limit0RemoveITCase extends BatchTestBase {

  @Before
  def before(): Unit = {
    lazy val numericData = Seq(
      row(null, 1L, 1.0f, 1.0d, JBigDecimal.valueOf(1)),
      row(2, null, 2.0f, 2.0d, JBigDecimal.valueOf(2)),
      row(3, 3L, null, 3.0d, JBigDecimal.valueOf(3)),
      row(3, 3L, 4.0f, null, JBigDecimal.valueOf(3))
    )

    registerCollection("t1", numericData, numericType, "a, b, c, d, e")
    registerCollection("t2", numericData, numericType, "a, b, c, d, e")
  }

  @Test
  def testSimpleLimitRemove(): Unit = {
    val sqlQuery = "SELECT * FROM t1 LIMIT 0"
    checkResult(sqlQuery, Seq())
  }

  @Test
  def testLimitRemoveWithOrderBy(): Unit = {
    val sqlQuery = "SELECT * FROM t1 ORDER BY a LIMIT 0"
    checkResult(sqlQuery, Seq())
  }

  @Test
  def testLimitRemoveWithJoin(): Unit = {
    val sqlQuery = "SELECT * FROM t1 JOIN (SELECT * FROM t2 LIMIT 0) ON true"
    checkResult(sqlQuery, Seq())
  }

  @Test
  def testLimitRemoveWithIn(): Unit = {
    val sqlQuery = "SELECT * FROM t1 WHERE a IN (SELECT a FROM t2 LIMIT 0)"
    checkResult(sqlQuery, Seq())
  }

  @Test
  def testLimitRemoveWithNotIn(): Unit = {
    val sqlQuery = "SELECT a FROM t1 WHERE a NOT IN (SELECT a FROM t2 LIMIT 0)"
    checkResult(sqlQuery, Seq(row(2), row(3), row(3), row(null)))
  }

  @Test(expected = classOf[TableException])
  // TODO remove exception after translateToPlanInternal is implemented in BatchExecNestedLoopJoin
  def testLimitRemoveWithExists(): Unit = {
    val sqlQuery = "SELECT * FROM t1 WHERE EXISTS (SELECT a FROM t2 LIMIT 0)"
    checkResult(sqlQuery, Seq())
  }

  @Test(expected = classOf[TableException])
  // TODO remove exception after translateToPlanInternal is implemented in BatchExecNestedLoopJoin
  def testLimitRemoveWithNotExists(): Unit = {
    val sqlQuery = "SELECT * FROM t1 WHERE NOT EXISTS (SELECT a FROM t2 LIMIT 0)"
    checkResult(sqlQuery, Seq(row(2), row(3), row(3), row(null)))
  }

  @Test
  def testLimitRemoveWithSelect(): Unit = {
    val sqlQuery = "SELECT * FROM (SELECT a FROM t2 LIMIT 0)"
    checkResult(sqlQuery, Seq())
  }

}
