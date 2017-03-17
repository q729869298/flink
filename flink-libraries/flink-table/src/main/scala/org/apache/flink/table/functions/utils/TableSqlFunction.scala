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

package org.apache.flink.table.functions.utils

import com.google.common.base.Predicate
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction
import org.apache.calcite.util.Util
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.plan.schema.FlinkTableFunctionImpl

import scala.collection.JavaConverters._
import java.util

import org.apache.flink.table.api.ValidationException

/**
  * Calcite wrapper for user-defined table functions.
  */
class TableSqlFunction(
    name: String,
    udtf: TableFunction[_],
    returnTypeInference: SqlReturnTypeInference,
    operandTypeInference: SqlOperandTypeInference,
    operandTypeChecker: SqlOperandTypeChecker,
    paramTypes: util.List[RelDataType],
    functionImpl: FlinkTableFunctionImpl[_])
  extends SqlUserDefinedTableFunction(
    new SqlIdentifier(name, SqlParserPos.ZERO),
    returnTypeInference,
    operandTypeInference,
    operandTypeChecker,
    paramTypes,
    functionImpl) {

  /**
    * Get the user-defined table function.
    */
  def getTableFunction = udtf

  /**
    * Get the type information of the table returned by the table function.
    */
  def getRowTypeInfo = if (null == functionImpl.resultType) {
    throw new ValidationException("The Result Type hasn't been generated yet")
  } else {
    functionImpl.resultType
  }

  /**
    * Get additional mapping information if the returned table type is a POJO
    * (POJO types have no deterministic field order).
    */
  def getPojoFieldMapping = if (null == functionImpl.resultType) {
    throw new ValidationException("The Result Type hasn't been generated yet")
  } else {
    functionImpl.fieldIndexes
  }

}

object TableSqlFunction {

  /**
    * Util function to create a [[TableSqlFunction]].
    *
    * @param name function name (used by SQL parser)
    * @param udtf user-defined table function to be called
    * @param typeFactory type factory for converting Flink's between Calcite's types
    * @param functionImpl Calcite table function schema
    * @return [[TableSqlFunction]]
    */
  def apply(
    name: String,
    udtf: TableFunction[_],
    typeFactory: FlinkTypeFactory,
    functionImpl: FlinkTableFunctionImpl[_]): TableSqlFunction = {

    val argTypes: util.List[RelDataType] = new util.ArrayList[RelDataType]
    val typeFamilies: util.List[SqlTypeFamily] = new util.ArrayList[SqlTypeFamily]
    // derives operands' data types and type families
    functionImpl.getParameters.asScala.foreach{ o =>
      val relType: RelDataType = o.getType(typeFactory)
      argTypes.add(relType)
      typeFamilies.add(Util.first(relType.getSqlTypeName.getFamily, SqlTypeFamily.ANY))
    }
    // derives whether the 'input'th parameter of a method is optional.
    val optional: Predicate[Integer] = new Predicate[Integer]() {
      def apply(input: Integer): Boolean = {
        functionImpl.getParameters.get(input).isOptional
      }
    }
    // create type check for the operands
    val typeChecker: FamilyOperandTypeChecker = OperandTypes.family(typeFamilies, optional)

    new TableSqlFunction(
      name,
      udtf,
      ReturnTypes.CURSOR,
      InferTypes.explicit(argTypes),
      typeChecker,
      argTypes,
      functionImpl)
  }
}
