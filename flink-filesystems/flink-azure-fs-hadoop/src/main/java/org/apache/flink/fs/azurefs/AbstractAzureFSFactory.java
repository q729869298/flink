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

package org.apache.flink.fs.azurefs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.runtime.util.HadoopUtils;

import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract factory for AzureFS. Subclasses override to specify
 * the correct scheme (wasb / wasbs). Based on Azure HDFS support in the
 * <a href="https://hadoop.apache.org/docs/current/hadoop-azure/index.html">hadoop-azure</a> module.
 *
 */
public abstract class AbstractAzureFSFactory implements FileSystemFactory {
	private static final Logger LOG = LoggerFactory.getLogger(AzureFSFactory.class);

	private static final String[] CONFIG_PREFIXES = { "fs.azure.", "azure." };

	private Configuration flinkConfig;

	@Override
	public void configure(Configuration config) {
		flinkConfig = config;
	}

	@Override
	public FileSystem create(URI fsUri) throws IOException {
		checkNotNull(fsUri, "passed file system URI object should not be null");
		LOG.info("Trying to load and instantiate Azure File System");
		return new HadoopFileSystem(createInitializedAzureFS(fsUri, flinkConfig));
	}

	// uri is of the form: wasb(s)://yourcontainer@youraccount.blob.core.windows.net/testDir
	private org.apache.hadoop.fs.FileSystem createInitializedAzureFS(URI fsUri, Configuration flinkConfig) throws IOException {
		org.apache.hadoop.conf.Configuration hadoopConfig = HadoopUtils.getHadoopConfiguration(flinkConfig);

		copyFlinkToHadoopConfig(flinkConfig, hadoopConfig);

		org.apache.hadoop.fs.FileSystem azureFS = new NativeAzureFileSystem();
		azureFS.initialize(fsUri, hadoopConfig);

		return azureFS;
	}

	private void copyFlinkToHadoopConfig(Configuration flinkConfig, org.apache.hadoop.conf.Configuration hadoopConfig) {
		// add additional config entries from the Flink config to the Hadoop config
		for (String key : flinkConfig.keySet()) {
			for (String prefix : CONFIG_PREFIXES) {
				if (key.startsWith(prefix)) {
					String value = flinkConfig.getString(key, null);
					String newKey = "fs.azure." + key.substring(prefix.length());
					hadoopConfig.set(newKey, flinkConfig.getString(key, null));

					LOG.debug("Adding Flink config entry for {} as {}={} to Hadoop config for AzureFS", key, newKey, value);
				}
			}
		}
	}
}
