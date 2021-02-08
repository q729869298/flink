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

package org.apache.flink.table.runtime.util;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateBackend;

/** Utility to create a {@link StateTtlConfig} object. */
public class StateConfigUtil {

    private static final String ROCKSDB_KEYED_STATE_BACKEND =
            "org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend";

    /**
     * Creates a {@link StateTtlConfig} depends on retentionTime parameter.
     *
     * @param retentionTime State ttl time which unit is MILLISECONDS.
     */
    public static StateTtlConfig createTtlConfig(long retentionTime) {
        if (retentionTime > 0) {
            return StateTtlConfig.newBuilder(Time.milliseconds(retentionTime))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
        } else {
            return StateTtlConfig.DISABLED;
        }
    }

    public static boolean isStateImmutableInStateBackend(KeyedStateBackend<?> stateBackend) {
        // TODO: remove the hard code check once FLINK-21027 is supported
        // state key and value is immutable only when using rocksdb state backend and timer
        boolean isRocksDbState =
                ROCKSDB_KEYED_STATE_BACKEND.equals(stateBackend.getClass().getCanonicalName());
        boolean isHeapTimer = false;
        if (stateBackend instanceof AbstractKeyedStateBackend) {
            // currently, requiresLegacySynchronousTimerSnapshots()
            // indicates the underlying uses heap-bsased timer
            isHeapTimer =
                    ((AbstractKeyedStateBackend<?>) stateBackend)
                            .requiresLegacySynchronousTimerSnapshots(CheckpointType.CHECKPOINT);
        }
        return isRocksDbState && !isHeapTimer;
    }
}
