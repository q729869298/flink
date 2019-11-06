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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.Field;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * A table schema that represents a table's structure with field names and data types.
 */
@PublicEvolving
public class TableSchema {

	private static final String ATOMIC_TYPE_FIELD_NAME = "f0";

	private final List<TableColumn> columns;

	private final List<WatermarkSpec> watermarkSpecs;

	private TableSchema(List<TableColumn> columns, List<WatermarkSpec> watermarkSpecs) {
		this.columns = Preconditions.checkNotNull(columns);
		this.watermarkSpecs = Preconditions.checkNotNull(watermarkSpecs);
	}

	/**
	 * @deprecated Use the {@link Builder} instead.
	 */
	@Deprecated
	public TableSchema(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		this(createTableColumns(fieldNames, fieldTypes), Collections.emptyList());
	}

	/**
	 * Returns a deep copy of the table schema.
	 */
	public TableSchema copy() {
		return new TableSchema(new ArrayList<>(columns), new ArrayList<>(watermarkSpecs));
	}

	/**
	 * Returns all field data types as an array.
	 */
	public DataType[] getFieldDataTypes() {
		return columns.stream()
			.map(TableColumn::getType)
			.toArray(DataType[]::new);
	}

	/**
	 * @deprecated This method will be removed in future versions as it uses the old type system. It
	 *             is recommended to use {@link #getFieldDataTypes()} instead which uses the new type
	 *             system based on {@link DataTypes}. Please make sure to use either the old or the new
	 *             type system consistently to avoid unintended behavior. See the website documentation
	 *             for more information.
	 */
	@Deprecated
	public TypeInformation<?>[] getFieldTypes() {
		return fromDataTypeToLegacyInfo(getFieldDataTypes());
	}

	/**
	 * Returns the specified data type for the given field index.
	 *
	 * @param fieldIndex the index of the field
	 */
	public Optional<DataType> getFieldDataType(int fieldIndex) {
		if (fieldIndex < 0 || fieldIndex >= columns.size()) {
			return Optional.empty();
		}
		return Optional.of(columns.get(fieldIndex).getType());
	}

	/**
	 * @deprecated This method will be removed in future versions as it uses the old type system. It
	 *             is recommended to use {@link #getFieldDataType(int)} instead which uses the new type
	 *             system based on {@link DataTypes}. Please make sure to use either the old or the new
	 *             type system consistently to avoid unintended behavior. See the website documentation
	 *             for more information.
	 */
	@Deprecated
	public Optional<TypeInformation<?>> getFieldType(int fieldIndex) {
		return getFieldDataType(fieldIndex)
			.map(TypeConversions::fromDataTypeToLegacyInfo);
	}

	/**
	 * Returns the specified data type for the given field name.
	 *
	 * @param fieldName the name of the field. the field name can be a nested field using a dot separator,
	 *                    e.g. "field1.innerField2"
	 */
	public Optional<DataType> getFieldDataType(String fieldName) {
		return this.columns.stream()
			.filter(column -> column.getName().equals(fieldName))
			.findFirst().map(TableColumn::getType);
	}

	/**
	 * @deprecated This method will be removed in future versions as it uses the old type system. It
	 *             is recommended to use {@link #getFieldDataType(String)} instead which uses the new type
	 *             system based on {@link DataTypes}. Please make sure to use either the old or the new
	 *             type system consistently to avoid unintended behavior. See the website documentation
	 *             for more information.
	 */
	@Deprecated
	public Optional<TypeInformation<?>> getFieldType(String fieldName) {
		return getFieldDataType(fieldName)
			.map(TypeConversions::fromDataTypeToLegacyInfo);
	}

	/**
	 * Returns the number of fields.
	 */
	public int getFieldCount() {
		return columns.size();
	}

	/**
	 * Returns all field names as an array.
	 */
	public String[] getFieldNames() {
		return this.columns.stream()
			.map(TableColumn::getName)
			.toArray(String[]::new);
	}

	/**
	 * Returns the specified name for the given field index.
	 *
	 * @param fieldIndex the index of the field
	 */
	public Optional<String> getFieldName(int fieldIndex) {
		if (fieldIndex < 0 || fieldIndex >= columns.size()) {
			return Optional.empty();
		}
		return Optional.of(this.columns.get(fieldIndex).getName());
	}

	/**
	 * Returns the {@link TableColumn} instance for the given field index.
	 *
	 * @param fieldIndex the index of the field
	 */
	public Optional<TableColumn> getTableColumn(int fieldIndex) {
		if (fieldIndex < 0 || fieldIndex >= columns.size()) {
			return Optional.empty();
		}
		return Optional.of(this.columns.get(fieldIndex));
	}

	/**
	 * Returns the {@link TableColumn} instance for the given field name.
	 *
	 * @param fieldName the name of the field
	 */
	public Optional<TableColumn> getTableColumn(String fieldName) {
		return this.columns.stream()
			.filter(column -> column.getName().equals(fieldName))
			.findFirst();
	}

	/**
	 * Returns all the {@link TableColumn}s for this table schema.
	 */
	public List<TableColumn> getTableColumns() {
		return new ArrayList<>(this.columns);
	}

	/**
	 * Converts a table schema into a (nested) data type describing a
	 * {@link DataTypes#ROW(Field...)}.
	 *
	 * <p>Note that the returned row type contains field types for all the columns, including
	 * normal columns and computed columns. Be caution with the computed column data types, because
	 * they are not expected to be included in the row type of TableSource or TableSink.
	 */
	public DataType toRowDataType() {
		final Field[] fields = columns.stream()
			.map(column -> FIELD(column.getName(), column.getType()))
			.toArray(Field[]::new);
		return ROW(fields);
	}

	/**
	 * @deprecated Use {@link #toRowDataType()} instead.
	 */
	@Deprecated
	@SuppressWarnings("unchecked")
	public TypeInformation<Row> toRowType() {
		return (TypeInformation<Row>) fromDataTypeToLegacyInfo(toRowDataType());
	}

	/**
	 * Returns a list of the watermark specification which contains rowtime attribute
	 * and watermark strategy expression.
	 *
	 * <p>NOTE: Currently, there is at most one {@link WatermarkSpec} in the list, because we don't
	 * support multiple watermarks definition yet. But in the future, we may support multiple watermarks.
	 */
	public List<WatermarkSpec> getWatermarkSpecs() {
		return watermarkSpecs;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("root\n");
		for (int i = 0; i < columns.size(); i++) {
			sb.append(" |-- ")
				.append(getFieldName(i).get())
				.append(": ");
			sb.append(getFieldDataType(i).get());
			if (columns.get(i).isGenerated()) {
				sb.append(" ").append(columns.get(i).getExpr().get());
			}
			sb.append('\n');
		}
		if (!watermarkSpecs.isEmpty()) {
			for (WatermarkSpec watermark : watermarkSpecs) {
				sb.append(" |-- ").append("WATERMARK FOR ")
					.append(watermark.getRowtimeAttribute()).append(" AS ")
					.append(watermark.getWatermarkExpressionString());
			}
		}
		return sb.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TableSchema schema = (TableSchema) o;
		return Objects.equals(columns, schema.columns)
			&& Objects.equals(watermarkSpecs, schema.getWatermarkSpecs());
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(columns);
		result = 31 * result + watermarkSpecs.hashCode();
		return result;
	}

	/**
	 * Creates a table schema from a {@link TypeInformation} instance. If the type information is
	 * a {@link CompositeType}, the field names and types for the composite type are used to
	 * construct the {@link TableSchema} instance. Otherwise, a table schema with a single field
	 * is created. The field name is "f0" and the field type the provided type.
	 *
	 * @param typeInfo The {@link TypeInformation} from which the table schema is generated.
	 * @return The table schema that was generated from the given {@link TypeInformation}.
	 *
	 * @deprecated This method will be removed soon. Use {@link DataTypes} to declare types.
	 */
	@Deprecated
	public static TableSchema fromTypeInfo(TypeInformation<?> typeInfo) {
		if (typeInfo instanceof CompositeType<?>) {
			final CompositeType<?> compositeType = (CompositeType<?>) typeInfo;
			// get field names and types from composite type
			final String[] fieldNames = compositeType.getFieldNames();
			final TypeInformation<?>[] fieldTypes = new TypeInformation[fieldNames.length];
			for (int i = 0; i < fieldTypes.length; i++) {
				fieldTypes[i] = compositeType.getTypeAt(i);
			}
			return new TableSchema(fieldNames, fieldTypes);
		} else {
			// create table schema with a single field named "f0" of the given type.
			return new TableSchema(
				new String[]{ATOMIC_TYPE_FIELD_NAME},
				new TypeInformation<?>[]{typeInfo});
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	//~ Tools ------------------------------------------------------------------

	/**
	 * Tools method to transform arrays of table names and types
	 * into a {@link TableColumn} list.
	 */
	private static List<TableColumn> createTableColumns(
			String[] fieldNames,
			TypeInformation<?>[] fieldTypes) {
			DataType[] fieldDataTypes = fromLegacyInfoToDataType(fieldTypes);
			validateFields(fieldNames, fieldDataTypes);
			List<TableColumn> columns = new ArrayList<>();
		for (int i = 0; i < fieldNames.length; i++) {
			columns.add(TableColumn.of(fieldNames[i], fieldDataTypes[i]));
		}
		return columns;
	}

	/** Fields sanity check. */
	private static void validateFields(String[] fieldNames, DataType[] fieldTypes) {
		if (fieldNames.length != fieldTypes.length) {
			throw new ValidationException(
				"Number of field names and field data types must be equal.\n" +
					"Number of names is " + fieldNames.length +
					", number of data types is " + fieldTypes.length + ".\n" +
					"List of field names: " + Arrays.toString(fieldNames) + "\n" +
					"List of field data types: " + Arrays.toString(fieldTypes));
		}
		// validate and create name to index mapping
		final Set<String> duplicateNames = new HashSet<>();
		final Set<String> uniqueNames = new HashSet<>();
		for (final String fieldName : fieldNames) {
			// check uniqueness of field names
			if (uniqueNames.contains(fieldName)) {
				duplicateNames.add(fieldName);
			} else {
				uniqueNames.add(fieldName);
			}
		}
		if (!duplicateNames.isEmpty()) {
			throw new ValidationException(
				"Field names must be unique.\n" +
					"List of duplicate fields: " + duplicateNames.toString() + "\n" +
					"List of all fields: " + Arrays.toString(fieldNames));
		}
	}

	/** Watermark specification sanity check. */
	private static void validateWatermarkSpecs(List<TableColumn> columns,
			List<WatermarkSpec> watermarkSpecs) {
		// Validate and create name to type mapping.
		// Field name to data type mapping, we need this because the row time attribute
		// field can be nested.
		final Map<String, DataType> fieldNameToType = new HashMap<>();
		for (TableColumn column : columns) {
			validateAndCreateNameToTypeMapping(fieldNameToType,
				column.getName(),
				column.getType(),
				"");
		}

		// Validate watermark and rowtime attribute.
		for (WatermarkSpec watermark : watermarkSpecs) {
			String rowtimeAttribute = watermark.getRowtimeAttribute();
			DataType rowtimeType = Optional.ofNullable(fieldNameToType.get(rowtimeAttribute))
				.orElseThrow(() -> new ValidationException(String.format(
					"Rowtime attribute '%s' is not defined in schema.", rowtimeAttribute)));
			if (rowtimeType.getLogicalType().getTypeRoot() != TIMESTAMP_WITHOUT_TIME_ZONE) {
				throw new ValidationException(String.format(
					"Rowtime attribute '%s' must be of type TIMESTAMP but is of type '%s'.",
					rowtimeAttribute, rowtimeType));
			}
			LogicalType watermarkOutputType = watermark.getWatermarkExprOutputType().getLogicalType();
			if (watermarkOutputType.getTypeRoot() != TIMESTAMP_WITHOUT_TIME_ZONE) {
				throw new ValidationException(String.format(
					"Watermark strategy '%s' must be of type TIMESTAMP but is of type '%s'.",
					watermark.getWatermarkExpressionString(),
					watermarkOutputType.asSerializableString()));
			}
		}
	}

	/**
	 * Creates a mapping from field name to data type, the field name can be a nested field.
	 * This is mainly used for validating whether the rowtime attribute (might be nested) exists
	 * in the schema. During creating, it also validates whether there is duplicate field names.
	 *
	 * <p>For example, a "f0" field of ROW type has two nested fields "q1" and "q2". Then the
	 * mapping will be ["f0" -> ROW, "f0.q1" -> INT, "f0.q2" -> STRING].
	 *
	 * <pre>
	 * {@code
	 *     f0 ROW<q1 INT, q2 STRING>
	 * }
	 * </pre>
	 *
	 * @param fieldNameToType Field name to type mapping that to update
	 * @param fieldName       Name of this field, e.g. "q1" or "q2" in the above example
	 * @param fieldType       Data type of this field
	 * @param parentFieldName Field name of parent type, e.g. "f0" in the above example
	 */
	private static void validateAndCreateNameToTypeMapping(
		Map<String, DataType> fieldNameToType,
		String fieldName,
		DataType fieldType,
		String parentFieldName) {
		String fullFieldName = parentFieldName.isEmpty() ? fieldName : parentFieldName + "." + fieldName;
		DataType oldType = fieldNameToType.put(fullFieldName, fieldType);
		if (oldType != null) {
			throw new ValidationException("Field names must be unique. Duplicate field: '" + fullFieldName + "'");
		}
		if (fieldType instanceof FieldsDataType) {
			Map<String, DataType> fieldDataTypes = ((FieldsDataType) fieldType).getFieldDataTypes();
			fieldDataTypes.forEach((key, value) ->
				validateAndCreateNameToTypeMapping(fieldNameToType, key, value, fullFieldName));
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Builder for creating a {@link TableSchema}.
	 */
	public static class Builder {

		private List<TableColumn> columns;

		private final List<WatermarkSpec> watermarkSpecs;

		public Builder() {
			columns = new ArrayList<>();
			watermarkSpecs = new ArrayList<>();
		}

		/**
		 * Add a field with name and data type.
		 *
		 * <p>The call order of this method determines the order of fields in the schema.
		 */
		public Builder field(String name, DataType dataType) {
			Preconditions.checkNotNull(name);
			Preconditions.checkNotNull(dataType);
			columns.add(TableColumn.of(name, dataType));
			return this;
		}

		/**
		 * Add a field with name and computation expression.
		 * This column would be a computed column.
		 *
		 * <p>The call order of this method determines the order of fields in the schema.
		 */
		public Builder field(String name, DataType dataType, String expression) {
			Preconditions.checkNotNull(name);
			Preconditions.checkNotNull(dataType);
			Preconditions.checkNotNull(expression);
			columns.add(TableColumn.of(name, dataType, expression));
			return this;
		}

		/**
		 * Add an array of fields with names and data types.
		 *
		 * <p>The call order of this method determines the order of fields in the schema.
		 */
		public Builder fields(String[] names, DataType[] dataTypes) {
			Preconditions.checkNotNull(names);
			Preconditions.checkNotNull(dataTypes);
			List<TableColumn> columns = IntStream.range(0, names.length)
				.mapToObj(idx -> TableColumn.of(names[idx], dataTypes[idx]))
				.collect(Collectors.toList());
			this.columns.addAll(columns);
			return this;
		}

		/**
		 * @deprecated This method will be removed in future versions as it uses the old type system. It
		 *             is recommended to use {@link #field(String, DataType)} instead which uses the new type
		 *             system based on {@link DataTypes}. Please make sure to use either the old or the new
		 *             type system consistently to avoid unintended behavior. See the website documentation
		 *             for more information.
		 */
		@Deprecated
		public Builder field(String name, TypeInformation<?> typeInfo) {
			return field(name, fromLegacyInfoToDataType(typeInfo));
		}

		/**
		 * Specifies the previously defined field as an event-time attribute and specifies the watermark strategy.
		 *
		 * @param rowtimeAttribute the field name as a rowtime attribute, can be a nested field using dot separator.
		 * @param watermarkExpressionString the string representation of watermark generation expression,
		 *                                  e.g. "ts - INTERVAL '5' SECOND". The string is a qualified SQL expression
		 *                                  string (UDFs are expanded) but will not be validated by {@link TableSchema}.
		 * @param watermarkExprOutputType the data type of the computation result of watermark generation expression.
		 *                                Whether the data type equals to the output type of expression will also
		 *                                not be validated by {@link TableSchema}.
		 */
		public Builder watermark(String rowtimeAttribute, String watermarkExpressionString, DataType watermarkExprOutputType) {
			Preconditions.checkNotNull(rowtimeAttribute);
			Preconditions.checkNotNull(watermarkExpressionString);
			Preconditions.checkNotNull(watermarkExprOutputType);
			if (!this.watermarkSpecs.isEmpty()) {
				throw new IllegalStateException("Multiple watermark definition is not supported yet.");
			}
			this.watermarkSpecs.add(new WatermarkSpec(rowtimeAttribute, watermarkExpressionString, watermarkExprOutputType));
			return this;
		}

		/**
		 * Returns a {@link TableSchema} instance.
		 */
		public TableSchema build() {
			validateFields(this.columns.stream().map(TableColumn::getName).toArray(String[]::new),
				this.columns.stream().map(TableColumn::getType).toArray(DataType[]::new));
			validateWatermarkSpecs(this.columns, this.watermarkSpecs);
			return new TableSchema(columns, watermarkSpecs);
		}
	}
}
