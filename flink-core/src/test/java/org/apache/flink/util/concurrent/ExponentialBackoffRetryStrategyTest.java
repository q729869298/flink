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

package org.apache.flink.util.concurrent;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ExponentialBackoffRetryStrategy}. */
public class ExponentialBackoffRetryStrategyTest {

    @Test
    void testGettersNotCapped() throws Exception {
        RetryStrategy retryStrategy =
                new ExponentialBackoffRetryStrategy(
                        10, Duration.ofMillis(5L), Duration.ofMillis(20L));
        assertThat(10, retryStrategy.getNumRemainingRetries());
        assertThat(Duration.ofMillis(5L), retryStrategy.getRetryDelay());

        RetryStrategy nextRetryStrategy = retryStrategy.getNextRetryStrategy();
        assertThat(9, nextRetryStrategy.getNumRemainingRetries());
        assertThat(Duration.ofMillis(10L), nextRetryStrategy.getRetryDelay());
    }

    @Test
    void testGettersHitCapped() throws Exception {
        RetryStrategy retryStrategy =
                new ExponentialBackoffRetryStrategy(
                        5, Duration.ofMillis(15L), Duration.ofMillis(20L));
        assertThat(5, retryStrategy.getNumRemainingRetries());
        assertThat(Duration.ofMillis(15L), retryStrategy.getRetryDelay());

        RetryStrategy nextRetryStrategy = retryStrategy.getNextRetryStrategy();
        assertThat(4, nextRetryStrategy.getNumRemainingRetries());
        assertThat(Duration.ofMillis(20L), nextRetryStrategy.getRetryDelay());
    }

    @Test
    void testGettersAtCap() throws Exception {
        RetryStrategy retryStrategy =
                new ExponentialBackoffRetryStrategy(
                        5, Duration.ofMillis(20L), Duration.ofMillis(20L));
        assertThat(5, retryStrategy.getNumRemainingRetries());
        assertThat(Duration.ofMillis(20L), retryStrategy.getRetryDelay());

        RetryStrategy nextRetryStrategy = retryStrategy.getNextRetryStrategy();
        assertThat(4, nextRetryStrategy.getNumRemainingRetries());
        assertThat(Duration.ofMillis(20L), nextRetryStrategy.getRetryDelay());
    }

    /** Tests that getting a next RetryStrategy below zero remaining retries fails. */
    @Test
    public void testRetryFailure() throws Throwable {
        assertThatThrownBy(
                        () ->
                                new ExponentialBackoffRetryStrategy(
                                                0, Duration.ofMillis(20L), Duration.ofMillis(20L))
                                        .getNextRetryStrategy())
                .isInstanceOf(IllegalStateException.class);
    }
}
