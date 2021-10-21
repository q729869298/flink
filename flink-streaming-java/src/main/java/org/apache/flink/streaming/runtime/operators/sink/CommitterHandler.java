/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * A wrapper around a {@link org.apache.flink.api.connector.sink.Committer} or {@link
 * org.apache.flink.api.connector.sink.GlobalCommitter} that manages states.
 *
 * @param <CommT> The type of the committable
 */
interface CommitterHandler<CommT> extends AutoCloseable {

    /** Initializes the state of the committer and this handler. */
    default void initializeState(StateInitializationContext context) throws Exception {}

    /** Snapshots the state of the committer and this handler. */
    default void snapshotState(StateSnapshotContext context) throws Exception {}

    /**
     * Processes the committables by either directly transforming them or by adding them to the
     * internal state of this handler.
     *
     * @return a list of output committables that is sent downstream.
     */
    Collection<CommT> processCommittables(Collection<CommT> committables);

    /**
     * Called when no more committables are going to be added through {@link
     * #processCommittables(Collection)}.
     *
     * @return a list of output committables that is sent downstream.
     */
    default Collection<CommT> endOfInput() throws IOException, InterruptedException {
        return Collections.emptyList();
    }

    /** Called when a checkpoint is completed and returns a list of output to be sent downstream. */
    default Collection<CommT> notifyCheckpointCompleted(long checkpointId)
            throws IOException, InterruptedException {
        return Collections.emptyList();
    }

    boolean needsRetry();

    /**
     * Retries all recovered committables. These committables may either be restored in {@link
     * #initializeState(StateInitializationContext)} and have been re-added in any of the committing
     * functions.
     *
     * @return successfully retried committables that is sent downstream.
     */
    Collection<CommT> retry() throws IOException, InterruptedException;
}
