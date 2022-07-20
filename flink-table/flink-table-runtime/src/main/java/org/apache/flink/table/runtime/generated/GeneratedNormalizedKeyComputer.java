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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;

/** Describes a generated {@link NormalizedKeyComputer}. */
public class GeneratedNormalizedKeyComputer extends GeneratedClass<NormalizedKeyComputer> {

    private static final long serialVersionUID = 2L;

    @VisibleForTesting
    public GeneratedNormalizedKeyComputer(String className, String code) {
        super(className, code, new Object[0], new Configuration());
    }

    /**
     * Creates a GeneratedNormalizedKeyComputer.
     *
     * @param className class name of the generated class.
     * @param code code of the generated class.
     * @param config configuration when generating the generated class.
     */
    public GeneratedNormalizedKeyComputer(String className, String code, ReadableConfig config) {
        super(className, code, new Object[0], config);
    }
}
