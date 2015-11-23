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

package org.apache.flink.api.table.expressions.analysis

import org.apache.flink.api.table.ExpressionException
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.expressions.analysis.FieldBacktracker
  .resolveFieldNameAndTableSource
import org.apache.flink.api.table.expressions.analysis.PredicatePruner.pruneExpr
import org.apache.flink.api.table.input.AdaptiveTableSource
import org.apache.flink.api.table.plan._
import org.apache.flink.api.table.trees.Rule

/**
 * Pushes constant predicates (e.g. a===12 && b.isNotNull) to each corresponding
 * AdaptiveTableSource.
 */
class PredicatePushdown(val inputOperation: PlanNode) extends Rule[Expression] {

  def apply(expr: Expression) = {
    // get all table sources where predicates can be push into
    val tableSources = getPushableTableSources(inputOperation)

    // prune expression tree such that it only contains constant predicates
    // such as a=1,a="Hello World", isNull(a) but not a=b
    val constantExpr = pruneExpr(isResolvedAndConstant, expr)

    // push predicates to each table source respectively
    for (ts <- tableSources) {
      // prune expression tree such that it only contains field references of ts
      val tsExpr = pruneExpr((e) => isSameTableSource(e, ts), constantExpr)

      // resolve field names to field names of the table source
      val result = tsExpr.transformPost {
        case rfr@ResolvedFieldReference(fieldName, typeInfo) =>
          // backtrack the field to its table source
          // each field should be backtrackable since we filtered
          // for field references of ts previously
          resolveFieldNameAndTableSource(inputOperation, fieldName) match {
            case Some(fieldWithSource) =>
              ResolvedFieldReference(fieldWithSource._2, typeInfo)
            case None => throw new ExpressionException("Field not found. This should not happen.")
          }
      }
      // push down predicates
      if (result != NopExpression()) {
        ts.notifyPredicates(result)
      }
    }
    expr
  }

  // ----------------------------------------------------------------------------------------------

  /**
   * @return all AdaptiveTableSources the given PlanNode contains
   */
  def getPushableTableSources(tree: PlanNode): Seq[AdaptiveTableSource] = tree match {
    case Root(ts: AdaptiveTableSource, _) => Seq(ts)
    case pn: PlanNode =>
      pn.children flatMap { child => getPushableTableSources(child ) }
    case _ => Seq() // add nothing
  }

  /**
   * 
   * @return true if the given expression is a predicate that consists of a
   *         ResolvedFieldReference and a constant/literal
   *         e.g. a=1, 2<c
   */
  def isResolvedAndConstant(expr: Expression) : Boolean = {
    expr match {
      case bc: BinaryComparison if bc.left.isInstanceOf[Literal]
          && bc.right.isInstanceOf[ResolvedFieldReference] =>
        true
      case bc: BinaryComparison if bc.right.isInstanceOf[Literal]
          && bc.left.isInstanceOf[ResolvedFieldReference] =>
        true
      case ue@(IsNotNull(_) | IsNull(_)) =>
        val child = ue.asInstanceOf[UnaryExpression].child
        child.isInstanceOf[ResolvedFieldReference]
      case And(_,_) | Or(_,_) | Not(_) =>
        true
      case _ => false
    }
  }

  /**
   * @return true if the given expression only consists of ResolvedFieldReference of
   *         the same given AdaptiveTableSource
   */
  def isSameTableSource(expr: Expression, ts: AdaptiveTableSource) : Boolean = {
    expr match {
      case bc: BinaryComparison if bc.right.isInstanceOf[ResolvedFieldReference] =>
        val fieldRef = bc.right.asInstanceOf[ResolvedFieldReference]
        val resolvedField = resolveFieldNameAndTableSource(inputOperation, fieldRef.name)
        resolvedField match {
          case Some(fieldWithSource) => fieldWithSource._1 == ts
          case None => false
        }
      case bc: BinaryComparison if bc.left.isInstanceOf[ResolvedFieldReference] =>
        val fieldRef = bc.left.asInstanceOf[ResolvedFieldReference]
        val resolvedField = resolveFieldNameAndTableSource(inputOperation, fieldRef.name)
        resolvedField match {
          case Some(fieldWithSource) => fieldWithSource._1 == ts
          case None => false
        }
      case ue@(IsNotNull(_) | IsNull(_)) =>
        val fieldRef = ue.asInstanceOf[UnaryExpression].child.asInstanceOf[ResolvedFieldReference]
        val resolvedField = resolveFieldNameAndTableSource(inputOperation, fieldRef.name)
        resolvedField match {
          case Some(fieldWithSource) => fieldWithSource._1 == ts
          case None => false
        }
      case _ => false
    }
  }
}
