/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.arrow.vectors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.dataformat.vector.DoubleColumnVector;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.Float8Vector;

/**
 * Arrow column vector for Double.
 */
@Internal
public final class ArrowDoubleColumnVector implements DoubleColumnVector {

	/**
	 * Container which is used to store the sequence of double values of a column to read.
	 */
	private final Float8Vector doubleVector;

	public ArrowDoubleColumnVector(Float8Vector doubleVector) {
		this.doubleVector = Preconditions.checkNotNull(doubleVector);
	}

	@Override
	public double getDouble(int i) {
		return doubleVector.get(i);
	}

	@Override
	public boolean isNullAt(int i) {
		return doubleVector.isNull(i);
	}
}
