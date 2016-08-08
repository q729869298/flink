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

package org.apache.flink.api.table.expressions

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.table.typeutils.IntervalTypeInfo

object ExpressionUtils {

  private[flink] def toMonthInterval(expr: Expression, multiplier: Int): Expression = expr match {
    case Literal(value: Int, BasicTypeInfo.INT_TYPE_INFO) =>
      Literal(value * multiplier, IntervalTypeInfo.INTERVAL_MONTHS)
    case _ =>
      Cast(Mul(expr, Literal(multiplier)), IntervalTypeInfo.INTERVAL_MONTHS)
  }

  private[flink] def toMilliInterval(expr: Expression, multiplier: Long): Expression = expr match {
    case Literal(value: Int, BasicTypeInfo.INT_TYPE_INFO) =>
      Literal(value * multiplier, IntervalTypeInfo.INTERVAL_MILLIS)
    case _ =>
      Cast(Mul(expr, Literal(multiplier)), IntervalTypeInfo.INTERVAL_MILLIS)
  }

}
