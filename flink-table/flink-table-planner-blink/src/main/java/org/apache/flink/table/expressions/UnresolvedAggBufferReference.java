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

package org.apache.flink.table.expressions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A reference to a unresolved aggregation reference field. The reference should be typed.
 * See more in {@link ResolvedAggLocalReference}.
 */
public class UnresolvedAggBufferReference implements Expression {

	private final String name;

	private final TypeInformation<?> resultType;

	public UnresolvedAggBufferReference(String name, TypeInformation<?> resultType) {
		this.name = Preconditions.checkNotNull(name);
		this.resultType = resultType;
	}

	public String getName() {
		return name;
	}

	public TypeInformation<?> getResultType() {
		return resultType;
	}

	@Override
	public List<Expression> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return visitor.visit(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		UnresolvedAggBufferReference that = (UnresolvedAggBufferReference) o;
		return Objects.equals(name, that.name) && Objects.equals(resultType, that.resultType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, resultType);
	}

	@Override
	public String toString() {
		return name;
	}
}
