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

import org.apache.flink.table.planner.runtime.stream.FsStreamingSinkITCaseBase

import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.Seq

/**
  * Test checkpoint for file system table factory with testcsv format.
  */
@RunWith(classOf[Parameterized])
class FsStreamingSinkTestCsvITCase(useBulkWriter: Boolean) extends FsStreamingSinkITCaseBase {

  override def additionalProperties(): Array[String] = {
    super.additionalProperties() ++
        Seq(
          "'format' = 'testcsv'",
          s"'testcsv.use-bulk-writer' = '$useBulkWriter'") ++
        (if (useBulkWriter) Seq() else Seq("'sink.rolling-policy.file-size' = '1b'"))
  }
}

object FsStreamingSinkTestCsvITCase {
  @Parameterized.Parameters(name = "useBulkWriter-{0}")
  def parameters(): java.util.Collection[Boolean] = {
    java.util.Arrays.asList(true, false)
  }
}
