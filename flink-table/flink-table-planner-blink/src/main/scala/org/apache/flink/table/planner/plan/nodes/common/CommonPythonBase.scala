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

package org.apache.flink.table.planner.plan.nodes.common

import org.apache.calcite.rex.{RexCall, RexLiteral, RexNode}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.configuration.{ConfigOption, Configuration, MemorySize, TaskManagerOptions}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.functions.FunctionDefinition
import org.apache.flink.table.functions.python.{PythonFunction, PythonFunctionInfo}
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.functions.utils.{ScalarSqlFunction, TableSqlFunction}
import org.apache.flink.table.planner.utils.DummyStreamExecutionEnvironment

import scala.collection.JavaConversions._
import scala.collection.mutable

trait CommonPythonBase {

  protected def loadClass(className: String): Class[_] = {
    try {
      Class.forName(className, false, Thread.currentThread.getContextClassLoader)
    } catch {
      case ex: ClassNotFoundException => throw new TableException(
        "The dependency of 'flink-python' is not present on the classpath.", ex)
    }
  }

  private lazy val convertLiteralToPython = {
    val clazz = loadClass("org.apache.flink.api.common.python.PythonBridgeUtils")
    clazz.getMethod("convertLiteralToPython", classOf[RexLiteral], classOf[SqlTypeName])
  }

  private def createPythonFunctionInfo(
      pythonRexCall: RexCall,
      inputNodes: mutable.Map[RexNode, Integer],
      functionDefinition: FunctionDefinition)
    : PythonFunctionInfo = {
    val inputs = new mutable.ArrayBuffer[AnyRef]()
    pythonRexCall.getOperands.foreach {
      case pythonRexCall: RexCall =>
        // Continuous Python UDFs can be chained together
        val argPythonInfo = createPythonFunctionInfo(pythonRexCall, inputNodes)
        inputs.append(argPythonInfo)

      case literal: RexLiteral =>
        inputs.append(
          convertLiteralToPython.invoke(null, literal, literal.getType.getSqlTypeName))

      case argNode: RexNode =>
        // For input arguments of RexInputRef, it's replaced with an offset into the input row
        inputNodes.get(argNode) match {
          case Some(existing) => inputs.append(existing)
          case None =>
            val inputOffset = Integer.valueOf(inputNodes.size)
            inputs.append(inputOffset)
            inputNodes.put(argNode, inputOffset)
        }
    }

    new PythonFunctionInfo(functionDefinition.asInstanceOf[PythonFunction], inputs.toArray)
  }

  protected def createPythonFunctionInfo(
      pythonRexCall: RexCall,
      inputNodes: mutable.Map[RexNode, Integer]): PythonFunctionInfo = {
    pythonRexCall.getOperator match {
      case sfc: ScalarSqlFunction =>
        createPythonFunctionInfo(pythonRexCall, inputNodes, sfc.scalarFunction)
      case tfc: TableSqlFunction =>
        createPythonFunctionInfo(pythonRexCall, inputNodes, tfc.udtf)
      case bsf: BridgingSqlFunction =>
        createPythonFunctionInfo(pythonRexCall, inputNodes, bsf.getDefinition)
    }
  }

  protected def getConfig(
      env: StreamExecutionEnvironment,
      tableConfig: TableConfig): Configuration = {
    val clazz = loadClass(CommonPythonBase.PYTHON_DEPENDENCY_UTILS_CLASS)
    val realEnv = getRealEnvironment(env)
    val method = clazz.getDeclaredMethod(
      "configurePythonDependencies", classOf[java.util.List[_]], classOf[Configuration])
    val config = method.invoke(
      null, realEnv.getCachedFiles, getMergedConfiguration(realEnv, tableConfig))
      .asInstanceOf[Configuration]
    config.setString("table.exec.timezone", tableConfig.getLocalTimeZone.getId)
    config
  }

  private def getMergedConfiguration(
      env: StreamExecutionEnvironment,
      tableConfig: TableConfig): Configuration = {
    // As the python dependency configurations may appear in both
    // `StreamExecutionEnvironment#getConfiguration` (e.g. parsed from flink-conf.yaml and command
    // line) and `TableConfig#getConfiguration` (e.g. user specified), we need to merge them and
    // ensure the user specified configuration has priority over others.
    val method = classOf[StreamExecutionEnvironment].getDeclaredMethod("getConfiguration")
    method.setAccessible(true)
    val executionEnvironmentConfig = method.invoke(env).asInstanceOf[Configuration]
    setDefaultConfigurations(executionEnvironmentConfig)
    val config = new Configuration(executionEnvironmentConfig)
    config.addAll(tableConfig.getConfiguration)
    config
  }

  private def getRealEnvironment(env: StreamExecutionEnvironment): StreamExecutionEnvironment = {
    val realExecEnvField = classOf[DummyStreamExecutionEnvironment].getDeclaredField("realExecEnv")
    realExecEnvField.setAccessible(true)
    var realEnv = env
    while (realEnv.isInstanceOf[DummyStreamExecutionEnvironment]) {
      realEnv = realExecEnvField.get(realEnv).asInstanceOf[StreamExecutionEnvironment]
    }
    realEnv
  }

  protected def usingManagedMemory(config: Configuration): Boolean = {
    val clazz = loadClass("org.apache.flink.python.PythonOptions")
    config.getBoolean(clazz.getField("USE_MANAGED_MEMORY").get(null)
      .asInstanceOf[ConfigOption[java.lang.Boolean]])
  }

  protected def getPythonWorkerMemory(config: Configuration): Long = {
    val clazz = loadClass("org.apache.flink.python.PythonOptions")
    val pythonFrameworkMemorySize = MemorySize.parse(
      config.getString(
        clazz.getField("PYTHON_FRAMEWORK_MEMORY_SIZE").get(null)
          .asInstanceOf[ConfigOption[String]]))
    val pythonBufferMemorySize = MemorySize.parse(
      config.getString(
        clazz.getField("PYTHON_DATA_BUFFER_MEMORY_SIZE").get(null)
          .asInstanceOf[ConfigOption[String]]))
    pythonFrameworkMemorySize.add(pythonBufferMemorySize).getBytes
  }

  private def setDefaultConfigurations(config: Configuration): Unit = {
    if (!usingManagedMemory(config) && !config.contains(TaskManagerOptions.TASK_OFF_HEAP_MEMORY)) {
      // Set the default value for the task off-heap memory if not set if the Python worker isn't
      // using managed memory. Note that this is best effort attempt and if a task slot contains
      // multiple Python workers, users should set the task off-heap memory manually.
      config.setString(TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key(),
        getPythonWorkerMemory(config) + "b")
    }
  }
}

object CommonPythonBase {
  val PYTHON_DEPENDENCY_UTILS_CLASS = "org.apache.flink.python.util.PythonDependencyUtils"
}
