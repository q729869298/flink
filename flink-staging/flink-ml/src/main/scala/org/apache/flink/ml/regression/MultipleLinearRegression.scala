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

package org.apache.flink.ml.regression

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.DataSet
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.JBlas._

import org.apache.flink.api.scala._

import org.jblas.{SimpleBlas, DoubleMatrix}

/**
 * Multiple linear regression using the ordinary least squares (OLS) estimator.
 *
 * The linear regression finds a solution to the problem
 *
 *    y = w0 + w1*x1 + w2*x2 ... + wn*xn = w0 + w^T*x
 *
 * such that the sum of squared residuals is minimized
 *
 *    min_{w, w0} = \sum (y - w^T*x - w0)^2
 *
 * The minimization problem is solved by (stochastic) gradient descent. For each labeled vector
 * (x,y), the gradient is calculated. The weighted average of all gradients is subtracted from
 * the current value of b which gives the new value of b. The weight is defined as
 * Stepsize/math.sqrt(iteration).
 *
 * The optimization runs at most Iterations or, if a ConvergenceThreshold has been set, until the
 * convergence criterion has been met. As convergence criterion the relative change of the sum of 
 * squared residuals is used:
 * 
 * Convergence if: (S_{k-1} - S_k)/S_{k-1} < ConvergenceThreshold
 * 
 * with S_k being the sum of squared residuals in iteration k.
 *
 * At the moment, the whole partition is used for SGD, making it effectively a batch gradient
 * descent. Once a sampling operator has been introduced, the algorithm can be optimized.
 * 
 * Parameters:
 * 
 *  - Iterations: Maximum number of iterations
 *
 *  - Stepsize: Initial stepsize for the gradient descent method. This value decides how far the
 *      gradient descent method goes in the direction of the gradient. Tuning this parameter can
 *      lead to better practical results.
 *
 *  - ConvergenceThreshold: Threshold for relative change of sum of squared residuals until
 *      convergence
 *  
 */
class MultipleLinearRegression extends Learner[LabeledVector, MultipleLinearRegressionModel]
with Serializable {
  import MultipleLinearRegression._

  def setIterations(iterations: Int): MultipleLinearRegression = {
    parameters.add(Iterations, iterations)
    this
  }

  def setStepsize(stepsize: Double): MultipleLinearRegression = {
    parameters.add(Stepsize, stepsize)
    this
  }

  def setConvergenceThreshold(convergenceThreshold: Double): MultipleLinearRegression = {
    parameters.add(ConvergenceThreshold, convergenceThreshold)
    this
  }

  override def fit(input: DataSet[LabeledVector], parameters: ParameterMap):
  MultipleLinearRegressionModel = {
    val map = this.parameters ++ parameters

    // retrieve parameters of the algorithm
    val numberOfIterations = map(Iterations)
    val stepsize = map(Stepsize)
    val convergenceThreshold = map.get(ConvergenceThreshold)

    // calculate dimension of the feature vectors
    val dimension = input.map{_.vector.size}.reduce { math.max(_, _) }

    // initial weight vector is set to 0
    val initialWeightVector = createInitialWeightVector(dimension)

    // check if a convergence threshold has been set
    val resultingWeightVector = convergenceThreshold match {
      case Some(convergence) =>

        // we have to calculate for each weight vector the sum of squared residuals
        val initialSquaredResidualSum = input.map {
          new SquaredResiduals
        }.withBroadcastSet(initialWeightVector, WEIGHTVECTOR_BROADCAST).reduce {
          _ + _
        }

        // combine weight vector with current sum of squared residuals
        val initialWeightVectorWithSquaredResidualSum = initialWeightVector.
          crossWithTiny(initialSquaredResidualSum).setParallelism(1)

        // start SGD iteration
        val resultWithResidual = initialWeightVectorWithSquaredResidualSum.
          iterateWithTermination(numberOfIterations) {
          weightVectorSquaredResidualDS =>

            // extract weight vector and squared residual sum
            val weightVector = weightVectorSquaredResidualDS.map{_._1}
            val squaredResidualSum = weightVectorSquaredResidualDS.map{_._2}

            // TODO: Sample from input to realize proper SGD
            val newWeightVector = input.map {
              new LinearRegressionGradientDescent
            }.withBroadcastSet(weightVector, WEIGHTVECTOR_BROADCAST).reduce {
              (left, right) =>
                  val (leftBetas, leftBeta0, leftCount) = left
                  val (rightBetas, rightBeta0, rightCount) = right

                  (leftBetas.add(rightBetas), leftBeta0 + rightBeta0, leftCount + rightCount)
            }.map {
              new LinearRegressionWeightsUpdate(stepsize)
            }.withBroadcastSet(weightVector, WEIGHTVECTOR_BROADCAST)

            // calculate the sum of squared residuals for the new weight vector
            val newResidual = input.map {
              new SquaredResiduals
            }.withBroadcastSet(newWeightVector, WEIGHTVECTOR_BROADCAST).reduce {
              _ + _
            }

            // check if the relative change in the squared residual sum is smaller than the
            // convergence threshold. If yes, then terminate => return empty termination data set
            val termination = squaredResidualSum.crossWithTiny(newResidual).setParallelism(1).
              filter{
              pair => {
                val (residual, newResidual) = pair

                if (residual <= 0) {
                  false
                } else {
                  math.abs((residual - newResidual)/residual) >= convergence
                }
              }
            }

            // result for new iteration
            (newWeightVector cross newResidual, termination)
        }

        // remove squared residual sum to only return the weight vector
        resultWithResidual.map{_._1}

      case None =>
        // No convergence criterion
        initialWeightVector.iterate(numberOfIterations) {
          weightVector => {

            // TODO: Sample from input to realize proper SGD
            input.map {
              new LinearRegressionGradientDescent
            }.withBroadcastSet(weightVector, WEIGHTVECTOR_BROADCAST).reduce {
              (left, right) =>
                val (leftBetas, leftBeta0, leftCount) = left
                val (rightBetas, rightBeta0, rightCount) = right

                (leftBetas.add(rightBetas), leftBeta0 + rightBeta0, leftCount + rightCount)
            }.map {
              new LinearRegressionWeightsUpdate(stepsize)
            }.withBroadcastSet(weightVector, WEIGHTVECTOR_BROADCAST)
          }
        }
    }

    new MultipleLinearRegressionModel(resultingWeightVector)
  }

  /**
   * Creates a DataSet with one zero vector. The zero vector has dimension d, which is given
   * by the dimensionDS.
   *
   * @param dimensionDS DataSet with one element d, denoting the dimension of the returned zero
   *                    vector
   * @return DataSet of a zero vector of dimension d
   */
  private def createInitialWeightVector(dimensionDS: DataSet[Int]):
  DataSet[(DoubleMatrix, Double)] = {
    dimensionDS.map {
      dimension =>
        val values = Array.fill(dimension)(0.0)
        (new DoubleMatrix(dimension, 1, values: _*), 0.0)
    }
  }
}

object MultipleLinearRegression {
  val WEIGHTVECTOR_BROADCAST = "weights_broadcast"

  // Define parameters for MultipleLinearRegression
  case object Stepsize extends Parameter[Double] {
    val defaultValue = Some(0.1)
  }

  case object Iterations extends Parameter[Int] {
    val defaultValue = Some(10)
  }

  case object ConvergenceThreshold extends Parameter[Double] {
    val defaultValue = None
  }
}

//--------------------------------------------------------------------------------------------------
//  Flink function definitions
//--------------------------------------------------------------------------------------------------

/**
 * Calculates for a labeled vector and the current weight vector its squared residual
 *
 *    (y - (w^Tx + w0))^2
 *
 * The weight vector is received as a broadcast variable.
 */
private class SquaredResiduals extends RichMapFunction[LabeledVector, Double] {
  import MultipleLinearRegression.WEIGHTVECTOR_BROADCAST

  var weightVector: DoubleMatrix = null
  var weight0: Double = 0.0

  @throws(classOf[Exception])
  override def open(configuration: Configuration): Unit = {
    val list = this.getRuntimeContext.
      getBroadcastVariable[(DoubleMatrix, Double)](WEIGHTVECTOR_BROADCAST)

    val weightsPair = list.get(0)

    weightVector = weightsPair._1
    weight0 = weightsPair._2
  }

  override def map(value: LabeledVector): Double = {
    val vector = value.vector
    val label = value.label

    val residual = weightVector.dot(vector) + weight0 - label

    residual*residual
  }
}

/**
 * Calculates for a labeled vector and the current weight vector the gradient minimizing the
 * OLS equation. The gradient is given by: 
 * 
 *    dw = 2*(w^T*x + w0 - y)*x
 *    dw0 = 2*(w^T*x + w0 - y)
 * 
 * The weight vector is received as a broadcast variable.
 */
private class LinearRegressionGradientDescent extends
RichMapFunction[LabeledVector, (DoubleMatrix, Double, Int)] {

  import MultipleLinearRegression.WEIGHTVECTOR_BROADCAST

  var weightVector: DoubleMatrix = null
  var weight0: Double = 0.0

  @throws(classOf[Exception])
  override def open(configuration: Configuration): Unit = {
    val list = this.getRuntimeContext.
      getBroadcastVariable[(DoubleMatrix, Double)](WEIGHTVECTOR_BROADCAST)

    val weightsPair = list.get(0)

    weightVector = weightsPair._1
    weight0 = weightsPair._2
  }

  override def map(value: LabeledVector): (DoubleMatrix, Double, Int) = {
    val x = value.vector
    val label = value.label

    val error = weightVector.dot(x) + weight0 - label

    val weightsGradient = x.mul(2 * error)
    val weight0Gradient = 2 * error

    (weightsGradient, weight0Gradient, 1)
  }
}

/**
 * Calculates the new weight vector based on the partial gradients. In order to do that,
 * all partial gradients are averaged and weighted by the current stepsize. This update value is
 * added to the current weight vector.
 * 
 * @param stepsize Initial value of the step size used to update the weight vector
 */
private class LinearRegressionWeightsUpdate(val stepsize: Double) extends
RichMapFunction[(DoubleMatrix, Double, Int), (DoubleMatrix, Double)] {

  import MultipleLinearRegression.WEIGHTVECTOR_BROADCAST

  var weights: DoubleMatrix = null
  var weight0: Double = 0.0

  @throws(classOf[Exception])
  override def open(configuration: Configuration): Unit = {
    val list = this.getRuntimeContext.
      getBroadcastVariable[(DoubleMatrix, Double)](WEIGHTVECTOR_BROADCAST)

    val weightsPair = list.get(0)

    weights = weightsPair._1
    weight0 = weightsPair._2
  }

  override def map(value: (DoubleMatrix, Double, Int)): (DoubleMatrix, Double) = {
    val weightsGradient = value._1.div(value._3)
    val weight0Gradient = value._2 / value._3

    val iteration = getIterationRuntimeContext.getSuperstepNumber

    // scale initial stepsize by the inverse square root of the iteration number to make it
    // decreasing
    val effectiveStepsize = stepsize/math.sqrt(iteration)

    val newWeights = new DoubleMatrix(weights.rows, weights.columns)
    newWeights.copy(weights)
    SimpleBlas.axpy( -effectiveStepsize, weightsGradient, newWeights)
    val newWeight0 = weight0 - effectiveStepsize * weight0Gradient

    (newWeights, newWeight0)
  }
}

//--------------------------------------------------------------------------------------------------
//  Model definition
//--------------------------------------------------------------------------------------------------

/**
 * Multiple linear regression model returned by [[MultipleLinearRegression]]. The model stores the
 * calculated weight vector and applies the linear model to given vectors v:
 *
 *    \hat{y} = w^T*v + w0
 *
 * with \hat{y} being the predicted regression value.
 * 
 * @param weights DataSet containing the calculated weight vector
 */
class MultipleLinearRegressionModel private[regression]
(val weights: DataSet[(DoubleMatrix, Double)]) extends
Transformer[ Vector, LabeledVector ] {

  import MultipleLinearRegression.WEIGHTVECTOR_BROADCAST

  // predict regression value for input
  override def transform(input: DataSet[Vector],
                         parameters: ParameterMap): DataSet[LabeledVector] = {
    input.map(new LinearRegressionPrediction).withBroadcastSet(weights, WEIGHTVECTOR_BROADCAST)
  }

  def squaredResidualSum(input: DataSet[LabeledVector]): DataSet[Double] = {
    input.map{
      new SquaredResiduals()
    }.withBroadcastSet(weights, WEIGHTVECTOR_BROADCAST).reduce{
      _ + _
    }
  }

  private class LinearRegressionPrediction extends RichMapFunction[Vector, LabeledVector] {
    private var weights: DoubleMatrix = null
    private var weight0: Double = 0


    @throws(classOf[Exception])
    override def open(configuration: Configuration): Unit = {
      val t = getRuntimeContext.getBroadcastVariable[(DoubleMatrix, Double)](WEIGHTVECTOR_BROADCAST)

      val weightsPair = t.get(0)

      weights = weightsPair._1
      weight0 = weightsPair._2
    }

    override def map(value: Vector): LabeledVector = {
      val prediction = weights.dot(value) + weight0

      LabeledVector(value, prediction)
    }
  }
}
