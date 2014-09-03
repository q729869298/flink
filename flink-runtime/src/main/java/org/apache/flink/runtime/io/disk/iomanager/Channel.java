/**
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


package org.apache.flink.runtime.io.disk.iomanager;

import java.io.File;
import java.util.Random;

import org.apache.flink.util.StringUtils;

/**
 * A Channel represents a collection of files that belong logically to the same resource. An example is a collection of
 * files that contain sorted runs of data from the same stream, that will later on be merged together.
 * 
 */
public final class Channel
{
	private static final int RANDOM_BYTES_LENGTH = 16;

	/**
	 * An ID identifying an underlying fileChannel.
	 * 
	 */
	public static class ID
	{
		private final String path;
		
		private final int threadNum;

		protected ID(final String path, final int threadNum) {
			this.path = path;
			this.threadNum = threadNum;
		}

		protected ID(final String basePath, final int threadNum, final Random random)
		{
			this.path = basePath + File.separator + randomString(random) + ".channel";
			this.threadNum = threadNum;
		}

		/**
		 * Returns the path to the underlying temporary file.
		 */
		public String getPath() {
			return path;
		}
		
		int getThreadNum() {
			return this.threadNum;
		}

		public String toString() {
			return path;
		}
	}

	public static final class Enumerator
	{
		private static final String FORMAT = "%s%s%s.%06d.channel";

		private final String[] paths;
		
		private final String namePrefix;

		private int counter;

		protected Enumerator(final String[] basePaths, final Random random)
		{
			this.paths = basePaths;
			this.namePrefix = randomString(random);
			this.counter = 0;
		}

		public ID next()
		{
			final int threadNum = counter % paths.length;
			return new ID(String.format(FORMAT, this.paths[threadNum], File.separator, namePrefix, (counter++)), threadNum);
		}
	}

	/**
	 * Creates a random byte sequence using the provided {@code random} generator and returns its hex representation.
	 * 
	 * @param random
	 *        The random number generator to be used.
	 * @return A hex representation of the generated byte sequence
	 */
	private static final String randomString(final Random random) {
		final byte[] bytes = new byte[RANDOM_BYTES_LENGTH];
		random.nextBytes(bytes);
		return StringUtils.byteToHexString(bytes);
	}
}
