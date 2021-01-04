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

package org.apache.flink.mesos.scheduler.messages;

import org.apache.mesos.Protos;

import java.io.Serializable;

/**
 * Message sent when an executor has exited/terminated. Note that any tasks running will have
 * TASK_LOST status updates automagically generated.
 */
public class ExecutorLost implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Protos.ExecutorID executorId;

    private final Protos.SlaveID slaveId;

    private final int status;

    public ExecutorLost(Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {
        this.executorId = executorId;
        this.slaveId = slaveId;
        this.status = status;
    }

    public Protos.ExecutorID executorId() {
        return executorId;
    }

    public Protos.SlaveID slaveId() {
        return slaveId;
    }

    public int status() {
        return status;
    }

    @Override
    public String toString() {
        return "ExecutorLost{"
                + "executorId="
                + executorId
                + ", slaveId="
                + slaveId
                + ", status="
                + status
                + '}';
    }
}
