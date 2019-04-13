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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.Internal;

/**
 * This interface provides a way for {@link TypeSerializer}s to transform a legacy {@link TypeSerializerSnapshot}
 * used in versions before Flink 1.7 during deserialization.
 */
@Internal
public interface LegacySerializerSnapshotTransformer<T> {

	/**
	 * Transform a {@link TypeSerializerSnapshot} that was previously associated with {@code this} {@link TypeSerializer}.
	 *
	 * @param legacySnapshot the snapshot to transform.
	 * @param <U> the legacy snapshot's serializer data type.
	 *
	 * @return a possibly transformed snapshot.
	 */
	<U> TypeSerializerSnapshot<T> transformLegacySerializerSnapshot(TypeSerializerSnapshot<U> legacySnapshot);
}
