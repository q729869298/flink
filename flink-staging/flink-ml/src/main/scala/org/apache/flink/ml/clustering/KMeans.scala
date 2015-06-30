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

package org.apache.flink.ml.clustering

import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.ml._
import org.apache.flink.ml.common.{LabeledVector, _}
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.{BLAS, Vector}
import org.apache.flink.ml.metrics.distances.EuclideanDistanceMetric
import org.apache.flink.ml.pipeline._


/**
 * Implements the KMeans algorithm which calculates cluster centroids based on set of training data
 * points and a set of k initial centroids.
 *
 * [[KMeans]] is a [[Predictor]] which needs to be trained on a set of data points and can then be
 * used to assign new points to the learned cluster centroids.
 *
 * The KMeans algorithm works as described on Wikipedia
 * (http://en.wikipedia.org/wiki/K-means_clustering):
 *
 * Given an initial set of k means m1(1),…,mk(1) (see below), the algorithm proceeds by alternating
 * between two steps:
 *
 * ===Assignment step:===
 *
 * Assign each observation to the cluster whose mean yields the least within-cluster sum  of
 * squares (WCSS). Since the sum of squares is the squared Euclidean distance, this is intuitively
 * the "nearest" mean. (Mathematically, this means partitioning the observations according to the
 * Voronoi diagram generated by the means).
 *
 * `S_i^(t) = { x_p : || x_p - m_i^(t) ||^2 ≤ || x_p - m_j^(t) ||^2 \forall j, 1 ≤ j ≤ k}`,
 * where each `x_p`  is assigned to exactly one `S^{(t)}`, even if it could be assigned to two or
 * more of them.
 *
 * ===Update step:===
 *
 * Calculate the new means to be the centroids of the observations in the new clusters.
 *
 * `m^{(t+1)}_i = ( 1 / |S^{(t)}_i| ) \sum_{x_j \in S^{(t)}_i} x_j`
 *
 * Since the arithmetic mean is a least-squares estimator, this also minimizes the within-cluster
 * sum of squares (WCSS) objective.
 *
 * @example
 * {{{
 *       val trainingDS: DataSet[Vector] = env.fromCollection(Clustering.trainingData)
 *       val initialCentroids: DataSet[LabledVector] = env.fromCollection(Clustering.initCentroids)
 *
 *       val kmeans = KMeans()
 *         .setInitialCentroids(initialCentroids)
 *         .setNumIterations(10)
 *
 *       kmeans.fit(trainingDS)
 *
 *       // getting the computed centroids
 *       val centroidsResult = kmeans.centroids.get.collect()
 *
 *       // get matching clusters for new points
 *       val testDS: DataSet[Vector] = env.fromCollection(Clustering.testData)
 *       val clusters: DataSet[LabeledVector] = kmeans.predict(testDS)
 * }}}
 *
 * =Parameters=
 *
 * - [[org.apache.flink.ml.clustering.KMeans.NumIterations]]:
 * Defines the number of iterations to recalculate the centroids of the clusters. As it
 * is a heuristic algorithm, there is no guarantee that it will converge to the global optimum. The
 * centroids of the clusters and the reassignment of the data points will be repeated till the
 * given number of iterations is reached.
 * (Default value: '''10''')
 *
 * - [[org.apache.flink.ml.clustering.KMeans.InitialCentroids]]:
 * Defines the initial k centroids of the k clusters. They are used as start off point of the
 * algorithm for clustering the data set. The centroids are recalculated as often as set in
 * [[org.apache.flink.ml.clustering.KMeans.NumIterations]]. The choice of the initial centroids
 * mainly affects the outcome of the algorithm.
 *
 */
class KMeans extends Predictor[KMeans] {

  import KMeans._

  /**
   * Stores the learned clusters after the fit operation
   */
  var centroids: Option[DataSet[LabeledVector]] = None

  /**
   * Sets the maximum number of iterations.
   *
   * @param numIterations The maximum number of iterations.
   * @return itself
   */
  def setNumIterations(numIterations: Int): KMeans = {
    parameters.add(NumIterations, numIterations)
    this
  }

  /**
   * Sets the initial centroids on which the algorithm will start computing. These points should
   * depend on the data and significantly influence the resulting centroids.
   *
   * @param initialCentroids A sequence of labeled vectors.
   * @return itself
   */
  def setInitialCentroids(initialCentroids: DataSet[LabeledVector]): KMeans = {
    parameters.add(InitialCentroids, initialCentroids)
    this
  }

}

/**
 * Companion object of KMeans. Contains convenience functions, the parameter type definitions
 * of the algorithm and the [[FitOperation]] & [[PredictOperation]].
 */
object KMeans {

  /** Euclidean Distance Metric */
  val euclidean = EuclideanDistanceMetric()

  case object NumIterations extends Parameter[Int] {
    val defaultValue = Some(10)
  }

  case object InitialCentroids extends Parameter[DataSet[LabeledVector]] {
    val defaultValue = None
  }

  // ========================================== Factory methods ====================================

  def apply(): KMeans = {
    new KMeans()
  }

  // ========================================== Operations =========================================

  /**
   * [[PredictOperation]] for vector types. The result type is a [[LabeledVector]].
   *
   * @return Anew [[PredictDataSetOperation]] to predict the labels of a [[DataSet]] of [[Vector]]s.
   * */
  implicit def predictDataSet = {
    new PredictDataSetOperation[KMeans, Vector, LabeledVector] {

      /** Calculates the predictions for all elements in the [[DataSet]] input
        *
        * @param instance Reference to the current KMeans instance.
        * @param predictParameters Container for predication parameter.
        * @param testDS Data set to make predict on.
        *
        * @return A [[DataSet[LabeledVectors]] containing the nearest centroids.
        */
      override def predictDataSet(instance: KMeans, predictParameters: ParameterMap,
                                  testDS: DataSet[Vector])
      : DataSet[LabeledVector] = {
        instance.centroids match {
          case Some(centroids) =>
            testDS.mapWithBcSet(centroids)
              { (dataPoint, centroids) => selectNearestCentroid(dataPoint, centroids) }
          case None =>
            throw new RuntimeException("The KMeans model has not been trained. Call first fit" +
              "before calling the predict operation.")
        }
      }
    }
  }

  /**
   * [[FitOperation]] which iteratively computes centroids that match the given input DataSet by
   * adjusting the given initial centroids.
   *
   * @return A new  [[FitOperation]] to train the model using the training data set.
   */
  implicit def fitKMeans = {
    new FitOperation[KMeans, Vector] {
      override def fit(instance: KMeans, fitParameters: ParameterMap, trainingDS: DataSet[Vector])
      : Unit = {
        val resultingParameters = instance.parameters ++ fitParameters

        val centroids: DataSet[LabeledVector] = resultingParameters.get(InitialCentroids).get
        val numIterations: Int = resultingParameters.get(NumIterations).get

        val finalCentroids = centroids.iterate(numIterations) { currentCentroids =>
          val newCentroids: DataSet[LabeledVector] = trainingDS
            .mapWithBcSet(currentCentroids)
              { (dataPoint, centroids) => selectNearestCentroid(dataPoint, centroids) }
            .map(x => (x.label, x.vector, 1.0)).withForwardedFields("label->_1; vector->_2")
            .groupBy(x => x._1)
            .reduce((p1, p2) =>
              (p1._1, (p1._2.asBreeze + p2._2.asBreeze).fromBreeze, p1._3 + p2._3))
            // TODO replace addition of Breeze vectors by future build in flink function
            .withForwardedFields("_1")
            .map(x => {
              BLAS.scal(1.0 / x._3, x._2)
              LabeledVector(x._1, x._2)
            })
            .withForwardedFields("_1->label")

          newCentroids
        }

        instance.centroids = Some(finalCentroids)
      }
    }
  }

  /**
   * Converts a given vector into a labeled vector where the label denotes the label of the closest
   * centroid.
   *
   * @param dataPoint The vector to determine the nearest centroid.
   * @param centroids A collection of the centroids.
   * @return A [[LabeledVector]] consisting of the input vector and the label of the closest
   *         centroid.
   */
  @ForwardedFields(Array("*->vector"))
  private def selectNearestCentroid(dataPoint: Vector, centroids: Traversable[LabeledVector]) = {
    var minDistance: Double = Double.MaxValue
    var closestCentroidLabel: Double = -1
    centroids.foreach(centroid => {
      val distance = euclidean.distance(dataPoint, centroid.vector)
      if (distance < minDistance) {
        minDistance = distance
        closestCentroidLabel = centroid.label
      }
    })
    LabeledVector(closestCentroidLabel, dataPoint)
  }

}
