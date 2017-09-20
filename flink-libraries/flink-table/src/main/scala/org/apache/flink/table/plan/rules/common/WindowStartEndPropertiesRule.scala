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

package org.apache.flink.table.plan.rules.common

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rex.{RexCall, RexNode}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.expressions.{WindowEnd, WindowStart}
import org.apache.flink.table.plan.logical.rel.LogicalWindowAggregate

import scala.collection.JavaConversions._

abstract class WindowStartEndPropertiesRule(ruleName: String, rulePredicate: RelOptRuleOperand)
  extends RelOptRule(rulePredicate, ruleName) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val project = call.rel(0).asInstanceOf[LogicalProject]
    // project includes at least on group auxiliary function

    def hasGroupAuxiliaries(node: RexNode): Boolean = {
      node match {
        case c: RexCall if c.getOperator.isGroupAuxiliary => true
        case c: RexCall =>
          c.operands.exists(hasGroupAuxiliaries)
        case _ => false
      }
    }

    project.getProjects.exists(hasGroupAuxiliaries)
  }

  def replaceGroupAuxiliaries(node: RexNode, transformed: RelBuilder): RexNode = {
    node match {
      case c: RexCall if isWindowStart(c) =>
        // replace expression by access to window start
        transformed.getRexBuilder.makeCast(c.getType, transformed.field("w$start"), false)
      case c: RexCall if isWindowEnd(c) =>
        // replace expression by access to window end
        transformed.getRexBuilder.makeCast(c.getType, transformed.field("w$end"), false)
      case c: RexCall =>
        // replace expressions in children
        val newOps = c.getOperands.map(x => replaceGroupAuxiliaries(x, transformed))
        c.clone(c.getType, newOps)
      case x =>
        // preserve expression
        x
    }
  }

  /** Checks if a RexNode is a window start auxiliary function. */
  private def isWindowStart(node: RexNode): Boolean = {
    node match {
      case n: RexCall if n.getOperator.isGroupAuxiliary =>
        n.getOperator match {
          case SqlStdOperatorTable.TUMBLE_START |
               SqlStdOperatorTable.HOP_START |
               SqlStdOperatorTable.SESSION_START
          => true
          case _ => false
        }
      case _ => false
    }
  }

  /** Checks if a RexNode is a window end auxiliary function. */
  private def isWindowEnd(node: RexNode): Boolean = {
    node match {
      case n: RexCall if n.getOperator.isGroupAuxiliary =>
        n.getOperator match {
          case SqlStdOperatorTable.TUMBLE_END |
               SqlStdOperatorTable.HOP_END |
               SqlStdOperatorTable.SESSION_END
          => true
          case _ => false
        }
      case _ => false
    }
  }
}

object WindowStartEndPropertiesRule {
  private val WINDOW_EXPRESSION_RULE_PREDICATE =
    RelOptRule.operand(classOf[LogicalProject],
      RelOptRule.operand(classOf[LogicalProject],
        RelOptRule.operand(classOf[LogicalWindowAggregate], RelOptRule.none())))

  val INSTANCE = new WindowStartEndPropertiesRule("WindowStartEndPropertiesRule" ,
    WINDOW_EXPRESSION_RULE_PREDICATE) {
    override def onMatch(call: RelOptRuleCall): Unit = {

      val project = call.rel(0).asInstanceOf[LogicalProject]
      val innerProject = call.rel(1).asInstanceOf[LogicalProject]
      val agg = call.rel(2).asInstanceOf[LogicalWindowAggregate]

      // Retrieve window start and end properties
      val transformed = call.builder()
      val rexBuilder = transformed.getRexBuilder
      transformed.push(LogicalWindowAggregate.create(
        agg.getWindow,
        Seq(
          NamedWindowProperty("w$start", WindowStart(agg.getWindow.aliasAttribute)),
          NamedWindowProperty("w$end", WindowEnd(agg.getWindow.aliasAttribute))
        ), agg)
      )

      // forward window start and end properties
      transformed.project(
        innerProject.getProjects ++ Seq(transformed.field("w$start"), transformed.field("w$end")))

      // replace window auxiliary function by access to window properties
      transformed.project(
        project.getProjects.map(x => replaceGroupAuxiliaries(x, transformed))
      )
      val res = transformed.build()
      call.transformTo(res)
    }
  }
}
