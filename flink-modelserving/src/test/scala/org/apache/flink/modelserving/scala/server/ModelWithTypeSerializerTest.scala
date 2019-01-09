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

package org.apache.flink.modelserving.scala.server

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.model.modeldescriptor.ModelDescriptor
import org.apache.flink.modelserving.java.server.SerializerTestBase
import org.apache.flink.modelserving.scala.model.{ModelWithType, ModelToServe,
  SimpleFactoryResolver}
import org.apache.flink.modelserving.scala.server.typeschema.ModelWithTypeSerializer

import java.io.File
import java.nio.file.{Files, Paths}

/**
  * Tests for the {@link ModelWithTypeSerializer}.
  */
class ModelWithTypeSerializerTest extends SerializerTestBase[ModelWithType]{

  private val tfmodeloptimized = "model/TF/optimized/optimized_WineQuality.pb"
  private val tfmodelsaved = "model/TF/saved/"
  private val pmmlmodel = "model/PMML/winequalityDecisionTreeClassification.pmml"

  private val dataType = "wine"

  ModelToServe.setResolver(new SimpleFactoryResolver)

  override protected def createSerializer(): TypeSerializer[ModelWithType] =
    new ModelWithTypeSerializer

  override protected def getLength: Int = -1

  override protected def getTypeClass: Class[ModelWithType] = classOf[ModelWithType]

  override protected def getTestData: Array[ModelWithType] = {

    // Get PMML model from File
    var model = getModel(pmmlmodel)
    // Create model from binary
    val pmml = ModelToServe.restore(ModelDescriptor.ModelType.PMML.value, model)
    // Get TF Optimized model from file
    model = getModel(tfmodeloptimized)
    val tfoptimized = ModelToServe.restore(ModelDescriptor.ModelType.TENSORFLOW.value, model)
    // Get TF bundled model location
    val classLoader = getClass.getClassLoader
    val file = new File(classLoader.getResource(tfmodelsaved).getFile)
    val location = file.getPath
    // Create model from location
    val tfbundled = ModelToServe.restore(ModelDescriptor.ModelType.TENSORFLOWSAVED.value,
      location.getBytes)

    Array[ModelWithType](
      new ModelWithType(false, dataType, pmml),
      new ModelWithType(false, dataType, Option.empty),
      new ModelWithType(false, dataType, tfoptimized),
      new ModelWithType(false, dataType, tfbundled))
  }

  private def getModel(fileName: String) : Array[Byte] = {
    val classLoader = getClass.getClassLoader
    val file = new File(classLoader.getResource(fileName).getFile)
    Files.readAllBytes(Paths.get(file.getPath))
  }
}
