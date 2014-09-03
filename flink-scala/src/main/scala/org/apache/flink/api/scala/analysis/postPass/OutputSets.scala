/**
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


package org.apache.flink.api.scala.analysis.postPass

import scala.language.reflectiveCalls
import scala.collection.JavaConversions._

import org.apache.flink.api.scala.analysis._

import org.apache.flink.compiler.dag._
import org.apache.flink.compiler.plan.OptimizedPlan


object OutputSets {

  import Extractors._

  def computeOutputSets(plan: OptimizedPlan): (Map[OptimizerNode, Set[Int]], Map[Int, GlobalPos]) = {
    
    val root = plan.getDataSinks.map(s => s.getSinkNode: OptimizerNode).reduceLeft((n1, n2) => new SinkJoiner(n1, n2))
    val outputSets = computeOutputSets(Map[OptimizerNode, Set[GlobalPos]](), root)
    val outputPositions = outputSets(root).map(pos => (pos.getValue, pos)).toMap
    
    (outputSets.mapValues(_.map(_.getValue)), outputPositions)
  }

  private def computeOutputSets(outputSets: Map[OptimizerNode, Set[GlobalPos]], node: OptimizerNode): Map[OptimizerNode, Set[GlobalPos]] = {

    outputSets.contains(node) match {

      case true => outputSets

      case false => {

        val children = node.getIncomingConnections.map(_.getSource).toSet
        val newOutputSets = children.foldLeft(outputSets)(computeOutputSets)
        
        val childOutputs = children.map(newOutputSets(_)).flatten
        val nodeOutputs = node.getUDF map { _.outputFields.filter(_.isUsed).map(_.globalPos).toSet } getOrElse Set()
        
        newOutputSets.updated(node, childOutputs ++ nodeOutputs)
      }
    }
  }
}
