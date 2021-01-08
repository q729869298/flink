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

package org.apache.flink.table.api.batch.sql.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.OverAgg0
import org.apache.flink.table.utils.TableTestBase

import org.junit.jupiter.api.Test

import java.sql.Timestamp

class OverWindowValidationTest extends TableTestBase {

  /**
    * OVER clause is necessary for [[OverAgg0]] window function.
    */
  @Test(expected = classOf[ValidationException])
  def testInvalidOverAggregation(): Unit = {

    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("T", 'a, 'b, 'c)

    util.addFunction("overAgg", new OverAgg0)

    val sqlQuery = "SELECT overAgg(b, a) FROM T"
    util.tableEnv.sqlQuery(sqlQuery)
  }

  /**
    * OVER clause is necessary for [[OverAgg0]] window function.
    */
  @Test(expected = classOf[ValidationException])
  def testInvalidOverAggregation2(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String, Timestamp)]("T", 'a, 'b, 'c, 'ts)
    util.addFunction("overAgg", new OverAgg0)

    val sqlQuery = "SELECT overAgg(b, a) FROM T GROUP BY TUMBLE(ts, INTERVAL '2' HOUR)"

    util.tableEnv.sqlQuery(sqlQuery)
  }
}
