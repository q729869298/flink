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

package org.apache.flink.python.util;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.datastream.runtime.operators.python.DataStreamPythonStatelessFunctionOperator;
import org.apache.flink.python.PythonConfig;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A Util class to get the {@link StreamExecutionEnvironment} configuration and merged configuration with environment
 * settings.
 */
public class PythonConfigUtil {

	public static final String KEYED_STREAM_VALUE_OPERATOR_NAME = "_keyed_stream_values_operator";
	public static final String STREAM_KEY_BY_MAP_OPERATOR_NAME = "_stream_key_by_map_operator";

	/**
	 * A static method to get the {@link StreamExecutionEnvironment} configuration merged with python dependency
	 * management configurations.
	 */
	public static Configuration getEnvConfigWithDependencies(StreamExecutionEnvironment env) throws InvocationTargetException,
		IllegalAccessException, NoSuchMethodException {
		Configuration envConfiguration = getEnvironmentConfig(env);
		Configuration config = PythonDependencyUtils.configurePythonDependencies(env.getCachedFiles(),
			envConfiguration);
		return config;
	}

	/**
	 * Get the private method {@link StreamExecutionEnvironment#getConfiguration()} by reflection recursively. Then
	 * access the method to get the configuration of the given StreamExecutionEnvironment.
	 */
	public static Configuration getEnvironmentConfig(StreamExecutionEnvironment env) throws InvocationTargetException,
		IllegalAccessException, NoSuchMethodException {
		Method getConfigurationMethod = null;
		for (Class<?> clz = env.getClass(); clz != Object.class; clz = clz.getSuperclass()) {
			try {
				getConfigurationMethod = clz.getDeclaredMethod("getConfiguration");
				break;
			} catch (NoSuchMethodException e) {

			}
		}

		if (getConfigurationMethod == null) {
			throw new NoSuchMethodException("Method getConfigurationMethod not found.");
		}

		getConfigurationMethod.setAccessible(true);
		Configuration envConfiguration = (Configuration) getConfigurationMethod.invoke(env);
		return envConfiguration;
	}

	/**
	 * Configure the {@link DataStreamPythonStatelessFunctionOperator} to be chained with the upstream/downstream
	 * operator by setting their parallelism, slot sharing group, co-location group to be the same, and applying a
	 * {@link ForwardPartitioner}.
	 * 1. operator with name "_keyed_stream_values_operator" should align with its downstream operator.
	 * 2. operator with name "_stream_key_by_map_operator" should align with its upstream operator.
	 */
	private static void alignStreamNode(StreamNode streamNode, StreamGraph streamGraph) {
		if (streamNode.getOperatorName().equals(KEYED_STREAM_VALUE_OPERATOR_NAME)) {
			StreamEdge downStreamEdge = streamNode.getOutEdges().get(0);
			StreamNode downStreamNode = streamGraph.getStreamNode(downStreamEdge.getTargetId());
			downStreamEdge.setPartitioner(new ForwardPartitioner());
			streamNode.setParallelism(downStreamNode.getParallelism());
			streamNode.setCoLocationGroup(downStreamNode.getCoLocationGroup());
			streamNode.setSlotSharingGroup(downStreamNode.getSlotSharingGroup());
		}

		if (streamNode.getOperatorName().equals(STREAM_KEY_BY_MAP_OPERATOR_NAME)) {
			StreamEdge upStreamEdge = streamNode.getInEdges().get(0);
			StreamNode upStreamNode = streamGraph.getStreamNode(upStreamEdge.getSourceId());
			upStreamEdge.setPartitioner(new ForwardPartitioner<>());
			streamNode.setParallelism(upStreamNode.getParallelism());
			streamNode.setSlotSharingGroup(upStreamNode.getSlotSharingGroup());
			streamNode.setCoLocationGroup(upStreamNode.getCoLocationGroup());
		}
	}

	/**
	 * Generate a {@link StreamGraph} for transformations maintained by current {@link StreamExecutionEnvironment}, and
	 * reset the merged env configurations with dependencies to every {@link DataStreamPythonStatelessFunctionOperator}.
	 * It is an idempotent operation that can be call multiple times. Remember that only when need to execute the
	 * StreamGraph can we set the clearTransformations to be True.
	 */
	public static StreamGraph generateStreamGraphWithDependencies(
		StreamExecutionEnvironment env, boolean clearTransformations) throws IllegalAccessException,
		NoSuchMethodException, InvocationTargetException {

		Configuration mergedConfig = getEnvConfigWithDependencies(env);
		StreamGraph streamGraph = env.getStreamGraph(StreamExecutionEnvironment.DEFAULT_JOB_NAME, clearTransformations);
		Collection<StreamNode> streamNodes = streamGraph.getStreamNodes();
		for (StreamNode streamNode : streamNodes) {

			alignStreamNode(streamNode, streamGraph);

			StreamOperatorFactory streamOperatorFactory = streamNode.getOperatorFactory();
			if (streamOperatorFactory instanceof SimpleOperatorFactory) {
				StreamOperator streamOperator = ((SimpleOperatorFactory) streamOperatorFactory).getOperator();
				if (streamOperator instanceof DataStreamPythonStatelessFunctionOperator) {
					DataStreamPythonStatelessFunctionOperator dataStreamPythonStatelessFunctionOperator =
						(DataStreamPythonStatelessFunctionOperator) streamOperator;
					Configuration oldConfig = dataStreamPythonStatelessFunctionOperator.getMergedEnvConfig();
					dataStreamPythonStatelessFunctionOperator.setPythonConfig(generateNewPythonConfig(oldConfig,
						mergedConfig));
				}
			}
		}
		return streamGraph;
	}

	/**
	 * Generator a new {@link  PythonConfig} with the combined config which is derived from oldConfig.
	 */
	private static PythonConfig generateNewPythonConfig(Configuration oldConfig, Configuration newConfig) {
		setIfNotExist(PythonOptions.MAX_BUNDLE_SIZE, oldConfig, newConfig);
		setIfNotExist(PythonOptions.MAX_BUNDLE_TIME_MILLS, oldConfig, newConfig);
		setIfNotExist(PythonOptions.MAX_BUNDLE_TIME_MILLS, oldConfig, newConfig);
		setIfNotExist(PythonOptions.PYTHON_FRAMEWORK_MEMORY_SIZE, oldConfig, newConfig);
		setIfNotExist(PythonOptions.PYTHON_DATA_BUFFER_MEMORY_SIZE, oldConfig, newConfig);
		setIfNotExist(PythonOptions.PYTHON_EXECUTABLE, oldConfig, newConfig);
		setIfNotExist(PythonOptions.PYTHON_METRIC_ENABLED, oldConfig, newConfig);
		setIfNotExist(PythonOptions.USE_MANAGED_MEMORY, oldConfig, newConfig);

		combineConfigValue(PythonDependencyUtils.PYTHON_FILES, oldConfig, newConfig);
		combineConfigValue(PythonDependencyUtils.PYTHON_REQUIREMENTS_FILE, oldConfig, newConfig);
		combineConfigValue(PythonDependencyUtils.PYTHON_ARCHIVES, oldConfig, newConfig);

		return new PythonConfig(oldConfig);
	}

	/**
	 * Make sure new configuration not overriding the previously configured value. For example, the MAX_BUNDLE_SIZE of
	 * {@link org.apache.flink.datastream.runtime.operators.python.DataStreamPythonReduceFunctionOperator} is
	 * pre-configured to be 1, we must not to change it.
	 */
	private static void setIfNotExist(ConfigOption configOption, Configuration oldConfig, Configuration newConfig) {
		if (!oldConfig.containsKey(configOption.key())) {
			oldConfig.set(configOption, newConfig.get(configOption));
		}
	}

	/**
	 * Dependency file information maintained by a Map in old config can be combined with new config.
	 */
	private static void combineConfigValue(ConfigOption<Map<String, String>> configOption, Configuration oldConfig, Configuration newConfig) {
		Map<String, String> oldConfigValue = oldConfig.getOptional(configOption).orElse(new HashMap<>());
		oldConfigValue.putAll(newConfig.getOptional(configOption).orElse(new HashMap<>()));
		oldConfig.set(configOption, oldConfigValue);
	}
}
