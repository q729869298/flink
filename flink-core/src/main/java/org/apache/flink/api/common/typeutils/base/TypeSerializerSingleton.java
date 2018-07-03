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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.ParameterlessTypeSerializerConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;

@Internal
public abstract class TypeSerializerSingleton<T> extends TypeSerializer<T>{

	private static final long serialVersionUID = 8766687317209282373L;

	// --------------------------------------------------------------------------------------------

	@Override
	public TypeSerializerSingleton<T> duplicate() {
		return this;
	}

	@Override
	public int hashCode() {
		return this.getClass().hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TypeSerializerSingleton) {
			TypeSerializerSingleton<?> other = (TypeSerializerSingleton<?>) obj;

			return other.canEqual(this);
		} else {
			return false;
		}
	}

	@Override
	public TypeSerializerConfigSnapshot<T> snapshotConfiguration() {
		// type serializer singletons should always be parameter-less
		return new ParameterlessTypeSerializerConfig<>(getSerializationFormatIdentifier());
	}

	@Override
	public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot<?> configSnapshot) {
		if (configSnapshot instanceof ParameterlessTypeSerializerConfig
				&& isCompatibleSerializationFormatIdentifier(
						((ParameterlessTypeSerializerConfig<?>) configSnapshot).getSerializationFormatIdentifier())) {

			return CompatibilityResult.compatible();
		} else {
			return CompatibilityResult.requiresMigration();
		}
	}

	/**
	 * Subclasses can override this if they know that they are also compatible with identifiers of other formats.
	 */
	protected boolean isCompatibleSerializationFormatIdentifier(String identifier) {
		return identifier.equals(getSerializationFormatIdentifier());
	}

	private String getSerializationFormatIdentifier() {
		return getClass().getCanonicalName();
	}
}
