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

package org.apache.flink.table.runtime.join

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row

/**
  * The function to execute processing time bounded stream inner-join.
  */
class ProcTimeBoundedStreamInnerJoin(
    leftLowerBound: Long,
    leftUpperBound: Long,
    allowedLateness: Long,
    leftType: TypeInformation[Row],
    rightType: TypeInformation[Row],
    genJoinFuncName: String,
    genJoinFuncCode: String)
    extends TimeBoundedStreamInnerJoin(
      leftLowerBound,
      leftUpperBound,
      allowedLateness,
      leftType,
      rightType,
      genJoinFuncName,
      genJoinFuncCode,
      leftTimeIdx = -1,
      rightTimeIdx = -1,
      JoinTimeIndicator.PROCTIME) {

  override def checkRowOutOfDate(timeForRow: Long, watermark: Long) = false

  override def updateOperatorTime(ctx: CoProcessFunction[CRow, CRow, CRow]#Context): Unit = {
    rightOperatorTime = ctx.timerService().currentProcessingTime()
    leftOperatorTime = ctx.timerService().currentProcessingTime()
  }

  override def getTimeForLeftStream(
      context: CoProcessFunction[CRow, CRow, CRow]#Context,
      row: CRow): Long = {
    context.timerService().currentProcessingTime()
  }

  override def getTimeForRightStream(
      context: CoProcessFunction[CRow, CRow, CRow]#Context,
      row: CRow): Long = {
    context.timerService().currentProcessingTime()
  }

  override def registerTimer(
      ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
      isLeft: Boolean,
      cleanupTime: Long): Unit = {
    ctx.timerService.registerProcessingTimeTimer(cleanupTime)
  }
}
