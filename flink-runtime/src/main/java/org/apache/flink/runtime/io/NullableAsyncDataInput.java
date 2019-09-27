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

package org.apache.flink.runtime.io;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

/**
 * The variant of {@link PullingAsyncDataInput} that for performance reasons returns {@code null}
 * from {@link #pollNextNullable()} instead returning {@code Optional.empty()} from
 * {@link PullingAsyncDataInput#pollNext()}.
 */
@Internal
public interface NullableAsyncDataInput<T> extends AvailabilityProvider {
	/**
	 * Poll the next element. This method should be non blocking.
	 *
	 * @return {@code null} will be returned if there is no data to return or
	 * if {@link #isFinished()} returns true. Otherwise return {@code element}.
	 */
	@Nullable
	T pollNextNullable() throws Exception;
}
