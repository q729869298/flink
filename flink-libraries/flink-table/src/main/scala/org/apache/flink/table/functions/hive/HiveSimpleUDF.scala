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
package org.apache.flink.table.functions.hive

import java.util

import org.apache.flink.table.functions.ScalarFunction
import org.apache.hadoop.hive.ql.exec.{FunctionRegistry, UDF}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ConversionHelper
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.typeinfo.{TypeInfo, TypeInfoFactory}

import scala.annotation.varargs

/**
  * A Hive UDF Wrapper which behaves as a Flink-table ScalarFunction.
  *
  * This class has to have a method with @varargs annotation. For scala will compile
  * <code> eval(args: Any*) </code> to <code>eval(args: Seq)</code>.
  * This will cause an exception in Janino compiler.
  */
class HiveSimpleUDF(name: String, functionWrapper: HiveFunctionWrapper) extends ScalarFunction {

  @transient
  private lazy val function = functionWrapper.createFunction[UDF]()

  @transient
  private var typeInfos: util.List[TypeInfo] = _

  @transient
  private var objectInspectors: Array[ObjectInspector] = _

  @transient
  private var conversionHelper: ConversionHelper = _

  @transient
  private val method = function.getResolver.getEvalMethod(typeInfos)

  @varargs
  def eval(args: Any*) : Any = {
    if (null == typeInfos) {
      typeInfos = new util.ArrayList[TypeInfo]()
      args.foreach(arg => {
          typeInfos.add(TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(arg.getClass))
      })
      objectInspectors = new Array[ObjectInspector](typeInfos.size())
      conversionHelper = new ConversionHelper(method, objectInspectors)
    }
    FunctionRegistry.invoke(method, function,
      conversionHelper.convertIfNecessary(args.toArray))
  }
}
