/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.scheduler.DefaultSchedulerFactory;
import org.apache.flink.runtime.scheduler.SchedulerNGFactory;
import org.apache.flink.runtime.scheduler.declarative.DeclarativeSchedulerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory for {@link SchedulerNGFactory}. */
public final class SchedulerNGFactoryFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SchedulerNGFactoryFactory.class);

    private SchedulerNGFactoryFactory() {}

    public static SchedulerNGFactory createSchedulerNGFactory(
            final Configuration configuration, JobType jobType) {
        JobManagerOptions.SchedulerType schedulerType =
                ClusterOptions.getSchedulerType(configuration);

        if (schedulerType == JobManagerOptions.SchedulerType.Declarative
                && jobType == JobType.BATCH) {
            LOG.info(
                    "Declarative Scheduler configured, but Batch job detected. Changing scheduler type to NG / DefaultScheduler.");
            // overwrite
            schedulerType = JobManagerOptions.SchedulerType.Ng;
        }

        switch (schedulerType) {
            case Ng:
                return new DefaultSchedulerFactory();
            case Declarative:
                return new DeclarativeSchedulerFactory();

            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Illegal value [%s] for config option [%s]",
                                schedulerType, JobManagerOptions.SCHEDULER.key()));
        }
    }
}
