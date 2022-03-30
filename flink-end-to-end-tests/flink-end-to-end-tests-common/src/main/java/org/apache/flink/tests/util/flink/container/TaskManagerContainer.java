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

package org.apache.flink.tests.util.flink.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

/** TaskManager container based on Testcontainers. */
class TaskManagerContainer extends GenericContainer<TaskManagerContainer> {

    private static final Logger LOG = LoggerFactory.getLogger(TaskManagerContainer.class);

    private final String taskManagerID;

    TaskManagerContainer(ImageFromDockerfile taskManagerImage, String taskManagerID) {
        super(taskManagerImage);
        this.taskManagerID = taskManagerID;
    }

    @Override
    public void start() {
        LOG.info("Starting TaskManager container with ID \"{}\"", taskManagerID);
        super.start();
    }

    @Override
    public void stop() {
        LOG.info("Stopping TaskManager container with ID \"{}\"", taskManagerID);
        super.stop();
    }
}
