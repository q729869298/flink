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
package org.apache.flink.api.table.plan.logical

import org.apache.flink.api.table.trees.TreeNode

abstract class LogicalNode extends TreeNode[LogicalNode] {
  def output: Seq[]
}

abstract class LeafNode extends LogicalNode {
  override def children: Seq[LogicalNode] = Nil
}

abstract class UnaryNode extends LogicalNode {
  def child: LogicalNode

  override def children: Seq[LogicalNode] = child :: Nil
}

abstract class BinaryNode extends LogicalNode {
  def left: LogicalNode
  def right: LogicalNode

  override def children: Seq[LogicalNode] = left :: right :: Nil
}
