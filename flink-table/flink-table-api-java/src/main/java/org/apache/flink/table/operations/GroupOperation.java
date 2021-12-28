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

package org.apache.flink.table.operations;

import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.stream.Collectors;

/** An {@link Operation} to represent a group of operations. */
public class GroupOperation implements Operation {

    private final List<Operation> operations;

    public GroupOperation(List<Operation> operations) {
        this.operations = Preconditions.checkNotNull(operations);
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "GroupOperation[%s]",
                operations.stream()
                        .map(Operation::asSummaryString)
                        .collect(Collectors.joining(",")));
    }

    public List<Operation> getOperations() {
        return operations;
    }
}
