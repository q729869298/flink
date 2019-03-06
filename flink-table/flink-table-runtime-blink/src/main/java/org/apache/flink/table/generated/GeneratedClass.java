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

package org.apache.flink.table.generated;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.Serializable;

/**
 * A wrapper for generated class, defines a {@link #newInstance(ClassLoader)} method
 * to get an instance by reference objects easily.
 */
public abstract class GeneratedClass<T> implements Serializable {

	private final String className;
	private final String code;
	private final Object[] references;

	private transient Class<T> compiledClass;

	protected GeneratedClass(String className, String code, Object[] references) {
		checkNotNull(className, "name must not be null");
		checkNotNull(code, "code must not be null");
		checkNotNull(references, "references must not be null");
		this.className = className;
		this.code = code;
		this.references = references;
	}

	/**
	 * Create a new instance of this generated class.
	 */
	@SuppressWarnings("unchecked")
	public T newInstance(ClassLoader classLoader) {
		try {
			return (T) compile(classLoader).getConstructor(Object[].class).newInstance(references);
		} catch (Exception e) {
			throw new RuntimeException(
				"Could not instantiate generated class '" + className + "'", e);
		}
	}

	private Class<?> compile(ClassLoader classLoader) {
		if (compiledClass == null) {
			// cache the compiled class
			compiledClass = CompileUtils.compile(classLoader, className, code);
		}
		return compiledClass;
	}


}
