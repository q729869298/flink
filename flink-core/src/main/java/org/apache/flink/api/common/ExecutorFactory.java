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

package org.apache.flink.api.common;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;

import java.net.URL;
import java.util.Collections;
import java.util.List;

/**
 * A ExecutorFactory create the specific implementation of {@link Executor}
 * 
 */
@Internal
public class ExecutorFactory<T extends Executor> {

	private static final String LOCAL_EXECUTOR_CLASS = "org.apache.flink.client.LocalExecutor";
	private static final String REMOTE_EXECUTOR_CLASS = "org.apache.flink.client.RemoteExecutor";

	private Class<T> clazz;

	public ExecutorFactory(Class<T> clazz) {
		this.clazz = clazz;
	}

	// ------------------------------------------------------------------------
	//  Executor Factories
	// ------------------------------------------------------------------------
	
	/**
	 * Creates an executor that runs locally in a multi-threaded environment.
	 * 
	 * @return A local executor.
	 */
	public T createLocalExecutor(Configuration configuration) {
		Class<? extends T> leClass = loadExecutorClass(LOCAL_EXECUTOR_CLASS);
		
		try {
			return leClass.getConstructor(Configuration.class).newInstance(configuration);
		}
		catch (Throwable t) {
			throw new RuntimeException("An error occurred while loading the local executor ("
					+ LOCAL_EXECUTOR_CLASS + ").", t);
		}
	}

	/**
	 * Creates an executor that runs on a remote environment. The remote executor is typically used
	 * to send the program to a cluster for execution.
	 *
	 * @param hostname The address of the JobManager to send the program to.
	 * @param port The port of the JobManager to send the program to.
	 * @param clientConfiguration The configuration for the client (Akka, default.parallelism).
	 * @param jarFiles A list of jar files that contain the user-defined function (UDF) classes and all classes used
	 *                 from within the UDFs.
	 * @param globalClasspaths A list of URLs that are added to the classpath of each user code classloader of the
	 *                 program. Paths must specify a protocol (e.g. file://) and be accessible on all nodes.
	 * @return A remote executor.
	 */
	public T createRemoteExecutor(String hostname, int port, Configuration clientConfiguration,
			List<URL> jarFiles, List<URL> globalClasspaths) {
		if (hostname == null) {
			throw new IllegalArgumentException("The hostname must not be null.");
		}
		if (port <= 0 || port > 0xffff) {
			throw new IllegalArgumentException("The port value is out of range.");
		}
		
		Class<? extends T> reClass = loadExecutorClass(REMOTE_EXECUTOR_CLASS);
		
		List<URL> files = (jarFiles == null) ?
				Collections.<URL>emptyList() : jarFiles;
		List<URL> paths = (globalClasspaths == null) ?
				Collections.<URL>emptyList() : globalClasspaths;

		try {
			T executor = (clientConfiguration == null) ?
					reClass.getConstructor(String.class, int.class, List.class)
						.newInstance(hostname, port, files) :
					reClass.getConstructor(String.class, int.class, Configuration.class, List.class, List.class)
						.newInstance(hostname, port, clientConfiguration, files, paths);
			return executor;
		}
		catch (Throwable t) {
			throw new RuntimeException("An error occurred while loading the remote executor ("
					+ REMOTE_EXECUTOR_CLASS + ").", t);
		}
	}
	
	private Class<? extends T> loadExecutorClass(String className) {
		try {
			Class<?> leClass = Class.forName(className);
			return leClass.asSubclass(clazz);
		}
		catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("Could not load the executor class (" + className
					+ "). Do you have the 'flink-clients' project in your dependencies?");
		}
		catch (Throwable t) {
			throw new RuntimeException("An error occurred while loading the executor (" + className + ").", t);
		}
	}
}
