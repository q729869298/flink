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
package org.apache.flink.table.codegen

import org.apache.flink.table.`type`.InternalType
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.generated.GeneratedCollector
import org.apache.flink.table.runtime.collector.TableFunctionCollector

/**
  * A code generator for generating [[org.apache.flink.util.Collector]]s.
  */
object CollectorCodeGenerator {

  /**
   * Generates a [[TableFunctionCollector]] that can be passed to Java compiler.
   *
   * @param ctx The context of the code generator
   * @param name Class name of the table function collector. Must not be unique but has to be a
   *             valid Java class identifier.
   * @param bodyCode body code for the collector method
   * @param inputType The type of the element being collected
   * @param collectedType The type of the element collected by the collector
   * @param inputTerm The term of the input element
   * @param collectedTerm The term of the collected element
   * @return instance of GeneratedCollector
   */
  def generateTableFunctionCollector(
      ctx: CodeGeneratorContext,
      name: String,
      bodyCode: String,
      inputType: InternalType,
      collectedType: InternalType,
      config: TableConfig,
      inputTerm: String = CodeGenUtils.DEFAULT_INPUT1_TERM,
      collectedTerm: String = CodeGenUtils.DEFAULT_INPUT2_TERM,
      converter: String => String = (a) => a): GeneratedCollector[TableFunctionCollector[_]] = {

    val className = newName(name)
    val input1TypeClass = boxedTypeTermForType(inputType)
    val input2TypeClass = boxedTypeTermForType(collectedType)

    val funcCode =
      j"""
      public class $className extends ${classOf[TableFunctionCollector[_]].getCanonicalName} {

        ${ctx.reuseMemberCode()}

        public $className() throws Exception {
          ${ctx.reuseInitCode()}
          ${ctx.reuseOpenCode()}
        }

        @Override
        public void collect(Object record) throws Exception {
          super.collect(record);
          $input1TypeClass $inputTerm = ($input1TypeClass) getInput();
          $input2TypeClass $collectedTerm = ($input2TypeClass) ${converter("record")};
          ${ctx.reuseLocalVariableCode()}
          ${ctx.reuseInputUnboxingCode()}
          ${ctx.reusePerRecordCode()}
          $bodyCode
        }

        @Override
        public void close() {
        }
      }
    """.stripMargin

    new GeneratedCollector(className, funcCode, ctx.references.toArray)
  }

}
