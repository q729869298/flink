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

package org.apache.flink.table.planner.functions.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.sql.SqlOperatorBinding;

import java.util.AbstractList;
import java.util.List;
import java.util.Optional;

/**
 * A {@link CallContext} backed by {@link SqlOperatorBinding}.
 */
@Internal
public final class OperatorBindingCallContext extends AbstractSqlCallContext {

	private final SqlOperatorBinding binding;

	private final List<DataType> argumentDataTypes;

	public OperatorBindingCallContext(
			DataTypeFactory dataTypeFactory,
			FunctionDefinition definition,
			SqlOperatorBinding binding) {
		super(
			dataTypeFactory,
			definition,
			binding.getOperator().getNameAsId().toString());

		this.binding = binding;
		this.argumentDataTypes = new AbstractList<DataType>() {
			@Override
			public DataType get(int pos) {
				final LogicalType logicalType = FlinkTypeFactory.toLogicalType(binding.getOperandType(pos));
				return TypeConversions.fromLogicalToDataType(logicalType);
			}

			@Override
			public int size() {
				return binding.getOperandCount();
			}
		};
	}

	@Override
	public boolean isArgumentLiteral(int pos) {
		return binding.isOperandLiteral(pos, false);
	}

	@Override
	public boolean isArgumentNull(int pos) {
		return binding.isOperandNull(pos, false);
	}

	@Override
	public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
		try {
			return Optional.ofNullable(
				getLiteralValueAs(
					new LiteralValueAccessor() {
						@Override
						public <R> R getValueAs(Class<R> clazz) {
							return binding.getOperandLiteralValue(pos, clazz);
						}
					},
					clazz));
		} catch (IllegalArgumentException e) {
			return Optional.empty();
		}
	}

	@Override
	public List<DataType> getArgumentDataTypes() {
		return argumentDataTypes;
	}

	@Override
	public Optional<DataType> getOutputDataType() {
		return Optional.empty();
	}
}
