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

package org.apache.flink.examples.modelserving.scala.model

import org.apache.flink.modelserving.wine.winerecord.WineRecord
import org.apache.flink.modelserving.scala.model.{Model, ModelFactory, ModelToServe}
import org.apache.flink.modelserving.scala.model.tensorflow.TensorFlowModel

import org.tensorflow.Tensor

/**
  * Implementation of tensorflow model (optimized) for wine.
  */
class WineTensorFlowModel(inputStream: Array[Byte]) extends TensorFlowModel(inputStream){

  /**
    * Score data.
    *
    * @param input object to score.
    */
  override def score(input: AnyVal): AnyVal = {

    // Convert input data
    val record = input.asInstanceOf[WineRecord]
    // Create input tensor
    val data = Array(
      record.fixedAcidity.toFloat,
      record.volatileAcidity.toFloat,
      record.citricAcid.toFloat,
      record.residualSugar.toFloat,
      record.chlorides.toFloat,
      record.freeSulfurDioxide.toFloat,
      record.totalSulfurDioxide.toFloat,
      record.density.toFloat,
      record.pH.toFloat,
      record.sulphates.toFloat,
      record.alcohol.toFloat
    )
    val modelInput = Tensor.create(Array(data))
    // Serve model using tensorflow APIs
    val result = session.runner.feed("dense_1_input", modelInput).fetch("dense_3/Sigmoid").run().
      get(0)
    // Get result shape
    val rshape = result.shape
    // Map output tensor to shape
    var rMatrix = Array.ofDim[Float](rshape(0).asInstanceOf[Int], rshape(1).asInstanceOf[Int])
    result.copyTo(rMatrix)
    // Get result
    var value = (0, rMatrix(0)(0))
    1 to (rshape(1).asInstanceOf[Int] - 1) foreach { i => {
      if (rMatrix(0)(i) > value._2) {
        value = (i, rMatrix(0)(i))
      }
    }}
    value._1.toDouble
  }
}

/**
  * Implementation of tensorflow (optimized) model factory for wine.
  */
object WineTensorFlowModel extends  ModelFactory {

  /**
    * Creates a new tensorflow (optimized) model.
    *
    * @param descriptor model to serve representation of tensorflow model.
    * @return model
    */
  override def create(input: ModelToServe): Option[Model] = try
    Some(new WineTensorFlowModel(input.model))
  catch {
    case t: Throwable => None
  }

  /**
    * Restore tensorflow (optimised) model from binary.
    *
    * @param bytes binary representation of tensorflow model.
    * @return model
    */
  override def restore(bytes: Array[Byte]) = new WineTensorFlowModel(bytes)
}
