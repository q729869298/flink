/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.io;

/**
 * This interface is implemented that allows for general compatibility checks between objects. For example, this can
 * be used to check if different versions of serializers are compatible with each other.
 * <p>
 * Notice that the relation isCompatible between two objects is (in general) not commutative and not transitive.
 *
 * @param <T> type of other instance that we want to check against for compatibility
 */
public interface Compatible<T> {

	/**
	 * Checks if this is compatible with the given argument. This relationship is between two objects is (in general)
	 * not commutative and not transitive.
	 *
	 * @param other the object to check for compatibility with
	 * @return true iff this is compatible with the passed argument
	 */
	boolean isCompatibleWith(T other);
}
