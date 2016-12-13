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

package org.apache.flink.api.table.plan.rules.util

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.JavaConverters._

object RexProgramProjectExtractor {

  /**
    * extract used input fields index of RexProgram
    *
    * @param rexProgram the RexProgram which to analyze
    * @return used input fields indices
    */
  def extractRefInputFields(rexProgram: RexProgram): Array[Int] = {
    val visitor = new RefFieldsVisitor
    // extract input fields from project expressions
    rexProgram.getProjectList.foreach(exp => rexProgram.expandLocalRef(exp).accept(visitor))
    val condition = rexProgram.getCondition
    // extract input fields from condition expression
    if (condition != null) {
      rexProgram.expandLocalRef(condition).accept(visitor)
    }
    visitor.getFields
  }

  /**
    * generate new RexProgram based on new input fields
    *
    * @param oldRexProgram   the old RexProgram
    * @param inputRowType    input row type
    * @param usedInputFields input fields index
    * @param rexBuilder      builder of rex expressions
    * @return new RexProgram which contains rewritten project expressions and
    *         rewritten condition expression
    */
  def rewriteRexProgram(
    oldRexProgram: RexProgram,
    inputRowType: RelDataType,
    usedInputFields: Array[Int],
    rexBuilder: RexBuilder): RexProgram = {
    val inputRewriter = new InputRewriter(usedInputFields)
    val newProjectExpressions = oldRexProgram.getProjectList.map(
      exp => oldRexProgram.expandLocalRef(exp).accept(inputRewriter)
    ).toList.asJava

    val oldCondition = oldRexProgram.getCondition
    val newConditionExpression = {
      oldCondition match {
        case ref: RexLocalRef => oldRexProgram.expandLocalRef(ref).accept(inputRewriter)
        case _ => null // null does not match any type
      }
    }
    RexProgram.create(
      inputRowType,
      newProjectExpressions,
      newConditionExpression,
      oldRexProgram.getOutputRowType,
      rexBuilder
    )
  }
}

/**
  * A RexVisitor to extract used input fields
  */
class RefFieldsVisitor extends RexVisitorImpl[Unit](true) {
  private var fields = mutable.LinkedHashSet[Int]()

  def getFields: Array[Int] = fields.toArray

  override def visitInputRef(inputRef: RexInputRef): Unit = fields += inputRef.getIndex

  override def visitCall(call: RexCall): Unit =
    call.operands.foreach(operand => operand.accept(this))
}

/**
  * This class is responsible for rewrite input
  *
  * @param fields fields mapping
  */
class InputRewriter(fields: Array[Int]) extends RexShuttle {

  /** old input fields ref index -> new input fields ref index mappings */
  private val fieldMap: Map[Int, Int] =
    fields.zipWithIndex.toMap

  override def visitInputRef(inputRef: RexInputRef): RexNode =
    new RexInputRef(relNodeIndex(inputRef), inputRef.getType)

  override def visitLocalRef(localRef: RexLocalRef): RexNode =
    new RexInputRef(relNodeIndex(localRef), localRef.getType)

  private def relNodeIndex(ref: RexSlot): Int =
    fieldMap.getOrElse(ref.getIndex,
      throw new IllegalArgumentException("input field contains invalid index"))
}
