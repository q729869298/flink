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

package org.apache.flink.core.failure;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** A {@link FailureEnricher} for testing purposes tracking Throwables and output failure labels. */
public class TestingFailureEnricher implements FailureEnricher {

    final Set<Throwable> seenThrowables = new HashSet<>();
    Map<String, String> failureLabels = Collections.singletonMap("failKey", "failValue");

    Set<String> outputKeys = Collections.singleton("failKey");

    @Override
    public Set<String> getOutputKeys() {
        return outputKeys;
    }

    @Override
    public CompletableFuture<Map<String, String>> processFailure(Throwable cause, Context context) {
        seenThrowables.add(cause);
        return CompletableFuture.completedFuture(failureLabels);
    }

    public Set<Throwable> getSeenThrowables() {
        return seenThrowables;
    }

    public Map<String, String> getFailureLabels() {
        return failureLabels;
    }

    public void setFailureLabels(Map<String, String> failureLabels) {
        this.failureLabels = failureLabels;
    }

    public void setOutputKeys(Set<String> outputKeys) {
        this.outputKeys = outputKeys;
    }
}
