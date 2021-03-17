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

package org.apache.flink.table.module;

import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Module of default core metadata in Flink. */
public class CoreModule implements Module {
    public static final CoreModule INSTANCE = new CoreModule();
    private List<BuiltInFunctionDefinition> builtInFunctionDefinitions;
    private Set<String> funcCache;

    private CoreModule() {
        this.builtInFunctionDefinitions = BuiltInFunctionDefinitions.getDefinitions();
        this.funcCache = new HashSet<>();
    }

    @Override
    public Set<String> listFunctions() {
        if (funcCache.isEmpty()) {
            funcCache =
                    builtInFunctionDefinitions.stream()
                            .map(f -> f.getName())
                            .collect(Collectors.toSet());
        }
        return funcCache;
    }

    @Override
    public Optional<FunctionDefinition> getFunctionDefinition(String name) {
        return builtInFunctionDefinitions.stream()
                .filter(f -> f.getName().equalsIgnoreCase(name))
                .findFirst()
                .map(Function.identity());
    }
}
