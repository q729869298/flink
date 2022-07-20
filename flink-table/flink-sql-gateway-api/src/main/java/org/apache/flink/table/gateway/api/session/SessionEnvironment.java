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

package org.apache.flink.table.gateway.api.session;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.gateway.api.endpoint.EndpointVersion;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Environment to initialize the {@code Session}. */
@PublicEvolving
public class SessionEnvironment {
    private final @Nullable String sessionName;
    private final EndpointVersion version;
    private final Map<String, String> sessionConfig;

    @VisibleForTesting
    SessionEnvironment(
            @Nullable String sessionName,
            EndpointVersion version,
            Map<String, String> sessionConfig) {
        this.sessionName = sessionName;
        this.version = version;
        this.sessionConfig = sessionConfig;
    }

    // -------------------------------------------------------------------------------------------
    // Getter
    // -------------------------------------------------------------------------------------------

    public Optional<String> getSessionName() {
        return Optional.ofNullable(sessionName);
    }

    public EndpointVersion getSessionEndpointVersion() {
        return version;
    }

    public Map<String, String> getSessionConfig() {
        return Collections.unmodifiableMap(sessionConfig);
    }

    // -------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SessionEnvironment)) {
            return false;
        }
        SessionEnvironment that = (SessionEnvironment) o;
        return Objects.equals(sessionName, that.sessionName)
                && Objects.equals(version, that.version)
                && Objects.equals(sessionConfig, that.sessionConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionName, version, sessionConfig);
    }

    // -------------------------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------------------------

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder to build the {@link SessionEnvironment}. */
    public static class Builder {
        private @Nullable String sessionName;
        private EndpointVersion version;
        private final Map<String, String> sessionConfig = new HashMap<>();

        public Builder setSessionName(String sessionName) {
            this.sessionName = sessionName;
            return this;
        }

        public Builder setSessionEndpointVersion(EndpointVersion version) {
            this.version = version;
            return this;
        }

        public Builder addSessionConfig(Map<String, String> sessionConfig) {
            this.sessionConfig.putAll(sessionConfig);
            return this;
        }

        public SessionEnvironment build() {
            return new SessionEnvironment(sessionName, checkNotNull(version), sessionConfig);
        }
    }
}
