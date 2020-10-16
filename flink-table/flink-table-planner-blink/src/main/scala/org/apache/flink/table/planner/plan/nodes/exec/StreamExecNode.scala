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

package org.apache.flink.table.planner.plan.nodes.exec

import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.utils.Logging

import java.util

/**
  * Base class for stream ExecNode.
  */
trait StreamExecNode[T] extends ExecNode[StreamPlanner, T] with Logging {

  def getInputEdges: util.List[ExecEdge] = {
    // TODO fill out the required shuffle for each stream exec node
    //  currently this interface is only used for deadlock breakup and multi-input in batch mode
    //  so we just put a placeholder implementation here
    val edges = new util.ArrayList[ExecEdge]()
    for (_ <- 0 until getInputNodes.size()) {
      edges.add(ExecEdge.builder()
        .damBehavior(ExecEdge.DamBehavior.PIPELINED)
        .build())
    }
    edges
  }
}
