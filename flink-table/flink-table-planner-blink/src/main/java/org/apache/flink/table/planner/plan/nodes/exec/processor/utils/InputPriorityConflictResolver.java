/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec.processor.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.AbstractExecNodeExactlyOnceVisitor;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.List;

/**
 * Subclass of the {@link InputPriorityGraphGenerator}.
 *
 * <p>This class resolve conflicts by inserting a {@link BatchExecExchange} into the conflicting
 * input.
 */
@Internal
public class InputPriorityConflictResolver extends InputPriorityGraphGenerator {

    private final ShuffleMode shuffleMode;
    private final Configuration configuration;

    /**
     * Create a {@link InputPriorityConflictResolver} for the given {@link ExecNode} graph.
     *
     * @param roots the first layer of nodes on the output side of the graph
     * @param safeDamBehavior when checking for conflicts we'll ignore the edges with {@link
     *     InputProperty.DamBehavior} stricter or equal than this
     * @param shuffleMode when a conflict occurs we'll insert an {@link BatchExecExchange} node with
     *     this shuffleMode to resolve conflict
     */
    public InputPriorityConflictResolver(
            List<ExecNode<?>> roots,
            InputProperty.DamBehavior safeDamBehavior,
            ShuffleMode shuffleMode,
            Configuration configuration) {
        super(roots, Collections.emptySet(), safeDamBehavior);
        this.shuffleMode = shuffleMode;
        this.configuration = configuration;
    }

    public void detectAndResolve() {
        createTopologyGraph();
    }

    @Override
    protected void resolveInputPriorityConflict(ExecNode<?> node, int higherInput, int lowerInput) {
        ExecNode<?> higherNode = node.getInputNodes().get(higherInput);
        ExecNode<?> lowerNode = node.getInputNodes().get(lowerInput);
        if (lowerNode instanceof BatchExecExchange) {
            BatchExecExchange exchange = (BatchExecExchange) lowerNode;
            InputProperty inputEdge = exchange.getInputProperties().get(0);
            InputProperty inputProperty =
                    InputProperty.builder()
                            .requiredDistribution(inputEdge.getRequiredDistribution())
                            .priority(inputEdge.getPriority())
                            .damBehavior(getDamBehavior())
                            .build();
            if (isConflictCausedByExchange(higherNode, exchange)) {
                // special case: if exchange is exactly the reuse node,
                // we should split it into two nodes
                BatchExecExchange newExchange =
                        new BatchExecExchange(
                                inputProperty, (RowType) exchange.getOutputType(), "Exchange");
                newExchange.setRequiredShuffleMode(shuffleMode);
                newExchange.setInputNodes(exchange.getInputNodes());
                node.replaceInputNode(lowerInput, newExchange);
            } else {
                // create new BatchExecExchange with new inputProperty
                BatchExecExchange newExchange =
                        new BatchExecExchange(
                                inputProperty,
                                (RowType) exchange.getOutputType(),
                                exchange.getDescription());
                newExchange.setRequiredShuffleMode(shuffleMode);
                newExchange.setInputNodes(exchange.getInputNodes());
                node.replaceInputNode(lowerInput, newExchange);
            }
        } else {
            node.replaceInputNode(lowerInput, createExchange(node, lowerInput));
        }
    }

    private boolean isConflictCausedByExchange(
            ExecNode<?> higherNode, BatchExecExchange lowerNode) {
        // check if `lowerNode` is the ancestor of `higherNode`,
        // if yes then conflict is caused by `lowerNode`
        ConflictCausedByExchangeChecker checker = new ConflictCausedByExchangeChecker(lowerNode);
        checker.visit(higherNode);
        return checker.found;
    }

    private BatchExecExchange createExchange(ExecNode<?> node, int idx) {
        ExecNode<?> inputNode = node.getInputNodes().get(idx);
        InputProperty inputProperty = node.getInputProperties().get(idx);
        InputProperty.RequiredDistribution requiredDistribution =
                inputProperty.getRequiredDistribution();
        if (requiredDistribution.getType() == InputProperty.DistributionType.BROADCAST) {
            // should not occur
            throw new IllegalStateException(
                    "Trying to resolve input priority conflict on broadcast side. This is not expected.");
        }

        InputProperty newInputProperty =
                InputProperty.builder()
                        .requiredDistribution(requiredDistribution)
                        .priority(inputProperty.getPriority())
                        .damBehavior(getDamBehavior())
                        .build();
        BatchExecExchange exchange =
                new BatchExecExchange(
                        newInputProperty, (RowType) inputNode.getOutputType(), "Exchange");
        exchange.setInputNodes(Collections.singletonList(inputNode));
        exchange.setRequiredShuffleMode(shuffleMode);
        return exchange;
    }

    private static class ConflictCausedByExchangeChecker
            extends AbstractExecNodeExactlyOnceVisitor {

        private final BatchExecExchange exchange;
        private boolean found;

        private ConflictCausedByExchangeChecker(BatchExecExchange exchange) {
            this.exchange = exchange;
        }

        @Override
        protected void visitNode(ExecNode<?> node) {
            if (node == exchange) {
                found = true;
            }
            for (ExecNode<?> inputNode : node.getInputNodes()) {
                visit(inputNode);
                if (found) {
                    return;
                }
            }
        }
    }

    private InputProperty.DamBehavior getDamBehavior() {
        if (BatchExecExchange.getShuffleMode(configuration, shuffleMode) == ShuffleMode.BATCH) {
            return InputProperty.DamBehavior.BLOCKING;
        } else {
            return InputProperty.DamBehavior.PIPELINED;
        }
    }
}
