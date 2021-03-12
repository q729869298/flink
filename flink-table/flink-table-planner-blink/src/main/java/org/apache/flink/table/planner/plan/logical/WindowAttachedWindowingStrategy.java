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

package org.apache.flink.table.planner.plan.logical;

import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A windowing strategy that gets windows from input columns as windows have been assigned and
 * attached to the physical columns.
 */
public class WindowAttachedWindowingStrategy extends WindowingStrategy {
    private final int windowStart;
    private final int windowEnd;

    public WindowAttachedWindowingStrategy(
            WindowSpec window, LogicalType timeAttributeType, int windowStart, int windowEnd) {
        super(window, timeAttributeType);
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toSummaryString(String[] inputFieldNames) {
        checkArgument(windowStart >= 0 && windowStart < inputFieldNames.length);
        String windowing =
                String.format(
                        "win_start=[%s], win_end=[%s]",
                        inputFieldNames[windowStart], inputFieldNames[windowEnd]);
        return window.toSummaryString(windowing);
    }

    public int getWindowStart() {
        return windowStart;
    }

    public int getWindowEnd() {
        return windowEnd;
    }
}
