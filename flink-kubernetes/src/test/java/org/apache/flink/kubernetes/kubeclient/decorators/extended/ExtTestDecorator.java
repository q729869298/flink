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

package org.apache.flink.kubernetes.kubeclient.decorators.extended;

import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;

import io.fabric8.kubernetes.api.model.HasMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** The test class for testing the plugin decorators. */
public class ExtTestDecorator implements ExtPluginDecorator {
    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        return flinkPod;
    }

    @Override
    public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
        return new ArrayList<>();
    }

    @Override
    public List<HasMetadata> buildPrePreparedResources() {
        return new ArrayList<>();
    }

    @Override
    public void configure(AbstractKubernetesParameters kubernetesComponentConf) {
        return;
    }
}
