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

package org.apache.flink.table.client.config;

import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.TableDescriptor;
import org.apache.flink.table.descriptors.TableDescriptorValidator;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Environment configuration that represents the content of an environment file. Environment files
 * define tables, execution, and deployment behavior. An environment might be defined by default or
 * as part of a session. Environments can be merged or enriched with properties (e.g. from CLI command).
 *
 * <p>In future versions, we might restrict the merging or enrichment of deployment properties to not
 * allow overwriting of a deployment by a session.
 */
public class Environment {

	private Map<String, TableDescriptor> tables;

	private Execution execution;

	private Deployment deployment;

	private static final String NAME = "name";

	public Environment() {
		this.tables = Collections.emptyMap();
		this.execution = new Execution();
		this.deployment = new Deployment();
	}

	public Map<String, TableDescriptor> getTables() {
		return tables;
	}

	private static TableDescriptor create(String name, Map<String, Object> config) {
		if (!config.containsKey(TableDescriptorValidator.TABLE_TYPE())) {
			throw new SqlClientException("The 'type' attribute of a table is missing.");
		}
		final String tableType = (String) config.get(TableDescriptorValidator.TABLE_TYPE());
		if (tableType.equals(TableDescriptorValidator.TABLE_TYPE_VALUE_SOURCE())) {
			return new Source(name, ConfigUtil.normalizeYaml(config));
		} else if (tableType.equals(TableDescriptorValidator.TABLE_TYPE_VALUE_SINK())) {
			return new Sink(name, ConfigUtil.normalizeYaml(config));
		} else if (tableType.equals(TableDescriptorValidator.TABLE_TYPE_VALUE_SOURCE_SINK())) {
			return new SourceSink(name, ConfigUtil.normalizeYaml(config));
		}
		return null;
	}

	public void setTables(List<Map<String, Object>> tables) {
		this.tables = new HashMap<>(tables.size());
		tables.forEach(config -> {
			if (!config.containsKey(NAME)) {
				throw new SqlClientException("The 'name' attribute of a table is missing.");
			}
			final Object name = config.get(NAME);
			if (name == null || !(name instanceof String) || ((String) name).length() <= 0) {
				throw new SqlClientException("Invalid table name '" + name + "'.");
			}
			final String tableName = (String) name;
			final Map<String, Object> properties = new HashMap<>(config);
			properties.remove(NAME);

			TableDescriptor tableDescriptor = create(tableName, properties);
			if (null == tableDescriptor) {
				throw new SqlClientException(
						"Invalid table 'type' attribute value, only 'source', 'sink' and 'both' is supported");
			}
			if (this.tables.containsKey(tableName)) {
				throw new SqlClientException("Duplicate table name '" + tableName + "'.");
			}
			this.tables.put(tableName, tableDescriptor);
		});
	}

	public void setExecution(Map<String, Object> config) {
		this.execution = Execution.create(config);
	}

	public Execution getExecution() {
		return execution;
	}

	public void setDeployment(Map<String, Object> config) {
		this.deployment = Deployment.create(config);
	}

	public Deployment getDeployment() {
		return deployment;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("===================== Tables =====================\n");
		tables.forEach((name, table) -> {
			sb.append("- name: ").append(name).append("\n");
			final DescriptorProperties props = new DescriptorProperties(true);
			table.addProperties(props);
			props.asMap().forEach((k, v) -> sb.append("  ").append(k).append(": ").append(v).append('\n'));
		});
		sb.append("=================== Execution ====================\n");
		execution.toProperties().forEach((k, v) -> sb.append(k).append(": ").append(v).append('\n'));
		sb.append("=================== Deployment ===================\n");
		deployment.toProperties().forEach((k, v) -> sb.append(k).append(": ").append(v).append('\n'));
		return sb.toString();
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Parses an environment file from an URL.
	 */
	public static Environment parse(URL url) throws IOException {
		return new ConfigUtil.LowerCaseYamlMapper().readValue(url, Environment.class);
	}

	/**
	 * Parses an environment file from an String.
	 */
	public static Environment parse(String content) throws IOException {
		return new ConfigUtil.LowerCaseYamlMapper().readValue(content, Environment.class);
	}

	/**
	 * Merges two environments. The properties of the first environment might be overwritten by the second one.
	 */
	public static Environment merge(Environment env1, Environment env2) {
		final Environment mergedEnv = new Environment();

		// merge tables
		final Map<String, TableDescriptor> tables = new HashMap<>(env1.getTables());
		tables.putAll(env2.getTables());
		mergedEnv.tables = tables;

		// merge execution properties
		mergedEnv.execution = Execution.merge(env1.getExecution(), env2.getExecution());

		// merge deployment properties
		mergedEnv.deployment = Deployment.merge(env1.getDeployment(), env2.getDeployment());

		return mergedEnv;
	}

	public static Environment enrich(Environment env, Map<String, String> properties) {
		final Environment enrichedEnv = new Environment();

		// merge tables
		enrichedEnv.tables = new HashMap<>(env.getTables());

		// enrich execution properties
		enrichedEnv.execution = Execution.enrich(env.execution, properties);

		// enrich deployment properties
		enrichedEnv.deployment = Deployment.enrich(env.deployment, properties);

		return enrichedEnv;
	}
}
