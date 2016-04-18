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
package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.core.fs.Path;

import java.io.Serializable;

/**
 * An interface to be implemented by the user when using the {@link FileSplitMonitoringFunction}.
 * The {@link #filterPath(Path)} method is responsible for deciding if a path is eligible for further
 * processing or not. This can serve to exclude temporary or partial files that
 * are still being written.
 *
 *<p/>
 * A default implementation is the {@link DefaultFilter} which excludes files starting with ".", "_", or
 * contain the "_COPYING_" in their names. This can be retrieved by {@link DefaultFilter#getInstance()}.
 * */
public interface FilePathFilter extends Serializable {

	/**
	 * Returns {@code true} if the {@code filePath} given is to be
	 * ignored when processing a directory, e.g.
	 * <pre>
	 * {@code
	 *
	 * public boolean filterPaths(Path filePath) {
	 *     return filePath.getName().startsWith(".") || filePath.getName().contains("_COPYING_");
	 * }
	 * }</pre>
	 * */
	boolean filterPath(Path filePath);

	/**
	 * The default file path filtering method and is used
	 * if no other such function is provided. This filter leaves out
	 * files starting with ".", "_", and "_COPYING_".
	 * */
	public class DefaultFilter implements FilePathFilter {

		private static DefaultFilter instance = null;

		DefaultFilter() {}

		public static DefaultFilter getInstance() {
			if (instance == null) {
				instance = new DefaultFilter();
			}
			return instance;
		}

		@Override
		public boolean filterPath(Path filePath) {
			return filePath == null ||
				filePath.getName().startsWith(".") ||
				filePath.getName().startsWith("_") ||
				filePath.getName().contains("_COPYING_");
		}
	}
}
