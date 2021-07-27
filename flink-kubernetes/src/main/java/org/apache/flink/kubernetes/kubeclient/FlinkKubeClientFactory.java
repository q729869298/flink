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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** A {@link FlinkKubeClientFactory} for creating the {@link FlinkKubeClient}. */
public class FlinkKubeClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkKubeClientFactory.class);

    private static final FlinkKubeClientFactory INSTANCE = new FlinkKubeClientFactory();

    public static FlinkKubeClientFactory getInstance() {
        return INSTANCE;
    }

    /**
     * Create a Flink Kubernetes client with the given configuration.
     *
     * @param flinkConfig Flink configuration
     * @param useCase Flink Kubernetes client use case (e.g. client, resourcemanager,
     *     kubernetes-ha-services)
     * @return Return the Flink Kubernetes client with the specified configuration and dedicated IO
     *     executor.
     */
    public FlinkKubeClient fromConfiguration(Configuration flinkConfig, String useCase) {
        final Config config;

        final String kubeContext = flinkConfig.getString(KubernetesConfigOptions.CONTEXT);
        if (kubeContext != null) {
            LOG.info("Configuring kubernetes client to use context {}.", kubeContext);
        }

        final String kubeConfigFile =
                flinkConfig.getString(KubernetesConfigOptions.KUBE_CONFIG_FILE);
        if (kubeConfigFile != null) {
            LOG.debug("Trying to load kubernetes config from file: {}.", kubeConfigFile);
            try {
                // If kubeContext is null, the default context in the kubeConfigFile will be used.
                // Note: the third parameter kubeconfigPath is optional and is set to null. It is
                // only used to rewrite
                // relative tls asset paths inside kubeconfig when a file is passed, and in the case
                // that the kubeconfig
                // references some assets via relative paths.
                config =
                        Config.fromKubeconfig(
                                kubeContext,
                                FileUtils.readFileUtf8(new File(kubeConfigFile)),
                                null);
            } catch (IOException e) {
                throw new KubernetesClientException("Load kubernetes config failed.", e);
            }
        } else {
            LOG.debug("Trying to load default kubernetes config.");

            config = Config.autoConfigure(kubeContext);
        }

        final String namespace = flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);
        LOG.debug("Setting namespace of Kubernetes client to {}", namespace);
        config.setNamespace(namespace);

        final NamespacedKubernetesClient client = new DefaultKubernetesClient(config);
        final int poolSize =
                flinkConfig.get(KubernetesConfigOptions.KUBERNETES_CLIENT_IO_EXECUTOR_POOL_SIZE);
        return new Fabric8FlinkKubeClient(
                flinkConfig, client, createThreadPoolForAsyncIO(poolSize, useCase));
    }

    private static ExecutorService createThreadPoolForAsyncIO(int poolSize, String useCase) {
        return Executors.newFixedThreadPool(
                poolSize, new ExecutorThreadFactory("flink-kubeclient-io-for-" + useCase));
    }
}
