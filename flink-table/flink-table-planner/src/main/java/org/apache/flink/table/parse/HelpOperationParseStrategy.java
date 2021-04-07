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

package org.apache.flink.table.parse;

import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.command.HelpOperation;

import java.util.regex.Pattern;

/** Strategy to parse statement to {@link HelpOperation}. */
public class HelpOperationParseStrategy extends AbstractRegexParseStrategy {

    static final HelpOperationParseStrategy INSTANCE = new HelpOperationParseStrategy();

    private HelpOperationParseStrategy() {
        super(Pattern.compile("HELP", DEFAULT_PATTERN_FLAGS));
    }

    @Override
    public Operation convert(String statement) {
        return new HelpOperation();
    }

    @Override
    public String[] getHints() {
        return new String[] {"HELP"};
    }
}
