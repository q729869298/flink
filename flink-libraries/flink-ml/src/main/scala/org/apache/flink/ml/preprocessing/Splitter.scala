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

package org.apache.flink.ml.preprocessing

import org.apache.flink.api.common.typeinfo.{TypeInformation, BasicTypeInfo}
import org.apache.flink.api.java.Utils
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala.utils._

import org.apache.flink.ml.common.{FlinkMLTools, ParameterMap, WithParameters}
import org.apache.flink.util.Collector
import _root_.scala.reflect.ClassTag

object Splitter {

  case class TrainTestDataSet[T: TypeInformation : ClassTag](training: DataSet[T],
                                                             testing: DataSet[T])

  case class TrainTestHoldoutDataSet[T: TypeInformation : ClassTag](training: DataSet[T],
                                                                    testing: DataSet[T],
                                                                    holdout: DataSet[T])
  // --------------------------------------------------------------------------------------------
  //  randomSplit
  // --------------------------------------------------------------------------------------------
  /**
   * Split a DataSet by the probability fraction of each element.
   *
   * @param input           DataSet to be split
   * @param fraction        Probability that each element is chosen, should be [0,1] without
   *                        replacement, and [0, ∞) with replacement. While fraction is larger
   *                        than 1, the elements are expected to be selected multi times into
   *                        sample on average. This fraction refers to the first element in the
   *                        resulting array.
   * @param precise         Sampling by default is random and can result in slightly lop-sided
   *                        sample sets. When precise is true, equal sample set size are forced,
   *                        however this is somewhat less efficient.
   * @param seed            Random number generator seed.
   * @return An array of two datasets
   */

  def randomSplit[T: TypeInformation : ClassTag](
      input: DataSet[T],
      fraction: Double,
      precise: Boolean = false,
      seed: Long = Utils.RNG.nextLong())
    : Array[DataSet[T]] = {
    import org.apache.flink.api.scala._

    val indexedInput: DataSet[(Long, T)] = input.zipWithUniqueId

    val leftSplit: DataSet[(Long, T)] = precise match {
      case false => indexedInput.sample(false, fraction, seed)
      case true => {
        val count = indexedInput.count()
        val numOfSamples = math.round(fraction * count).toInt
        indexedInput.sampleWithSize(false, numOfSamples, seed)
      }
    }

    val rightSplit: DataSet[T] = indexedInput.leftOuterJoin[(Long, T)](leftSplit)
      .where(0)
      .equalTo(0).apply {
        (full: (Long,T) , left: (Long, T), collector: Collector[T]) =>
        if (left == null) {
          collector.collect(full._2)
        }
    }
  }

  // --------------------------------------------------------------------------------------------
  //  multiRandomSplit
  // --------------------------------------------------------------------------------------------
  /**
   * Split a DataSet by the probability fraction of each element of a vector.
   *
   * @param input           DataSet to be split
   * @param fracArray       An array of PROPORTIONS for splitting the DataSet. Unlike the
   *                        randomSplit function, number greater than 1 do not lead to over
   *                        sampling. The number of splits is dictated by the length of this array.
   *                        The number are normalized, eg. Array(1.0, 2.0) would yield
   *                        two data sets with a 33/66% split.
   * @param precise         Sampling by default is random and can result in slightly lop-sided
   *                        sample sets. When precise is true, equal sample set size are forced,
   *                        however this is somewhat less efficient.
   * @param seed            Random number generator seed.
   * @return An array of DataSets whose length is equal to the length of fracArray
   */
  def multiRandomSplit[T: TypeInformation : ClassTag](
      input: DataSet[T],
      fracArray: Array[Double],
      precise: Boolean = false,
      seed: Long = Utils.RNG.nextLong())
    : Array[DataSet[T]] = {
    val splits = fracArray.length
    val output = new Array[DataSet[T]](splits)
    val aggs = fracArray.scanRight((0.0))( _ + _ )
    val fracs = fracArray.zip(aggs).map( o => o._1 / o._2)

    ////
    var tempDS = input
    for (k <- 0 to splits-2){
      var temp = Splitter.randomSplit(tempDS, fracs(k), true)
      output(k) = temp(0)
      tempDS = temp(1)
    }
    output(splits-1) = tempDS
    output
  }

  // --------------------------------------------------------------------------------------------
  //  kFoldSplit
  // --------------------------------------------------------------------------------------------
  /**
   * Split a DataSet into an array of TrainTest DataSets
   *
   * @param input           DataSet to be split
   * @param kFolds          The number of TrainTest DataSets to be returns. Each 'testing' will be
   *                        1/k of the dataset, randomly sampled, the training will be the remainder
   *                        of the dataset.  The DataSet is split into kFolds first, so that no
   *                        observation will occurin in multiple folds.
   * @param precise         Sampling by default is random and can result in slightly lop-sided
   *                        sample sets. When precise is true, equal sample set size are forced,
   *                        however this is somewhat less efficient.
   * @param seed            Random number generator seed.
   * @return An array of TrainTestDataSets
   */
  def kFoldSplit[T: TypeInformation : ClassTag](
      input: DataSet[T],
      kFolds: Int,
      precise: Boolean = false,
      seed: Long = Utils.RNG.nextLong())
    : Array[TrainTestDataSet[T]] = {

    val fracs = Array.fill(kFolds)(1.0)
    val dataSetArray = multiRandomSplit(input, fracs, precise, seed)

    dataSetArray.map( ds => TrainTestDataSet(ds,
                                             dataSetArray.filter(_ != ds)
                                                         .reduce(_ union _) ))

  }

  def unionDataSetArray[T: TypeInformation : ClassTag](dsa : Array[DataSet[T]]): DataSet[T] = {
    var dsu = dsa(0)
    for (k <- 1 to dsa.length-1) {
      dsu = dsu.union(dsa(k))
    }
    dsu
  }

  // --------------------------------------------------------------------------------------------
  //  trainTestSplit
  // --------------------------------------------------------------------------------------------
  /**
   * A wrapper for randomSplit that yields a TrainTestDataSet
   *
   * @param input           DataSet to be split
   * @param fraction        Probability that each element is chosen, should be [0,1] without
   *                        replacement, and [0, ∞) with replacement. While fraction is larger
   *                        than 1, the elements are expected to be selected multi times into
   *                        sample on average. This fraction refers to the training element in
   *                        TrainTestSplit
   * @param precise         Sampling by default is random and can result in slightly lop-sided
   *                        sample sets. When precise is true, equal sample set size are forced,
   *                        however this is somewhat less efficient.
   * @param seed            Random number generator seed.
   * @return A TrainTestDataSet
   */
  def trainTestSplit[T: TypeInformation : ClassTag](
      input: DataSet[T],
      fraction: Double = 0.6,
      precise: Boolean = false,
      seed: Long = Utils.RNG.nextLong())
    : TrainTestDataSet[T] = {
    val dataSetArray = randomSplit(input, fraction, precise, seed)
    TrainTestDataSet(dataSetArray(0), dataSetArray(1))
  }

  // --------------------------------------------------------------------------------------------
  //  trainTestHoldoutSplit
  // --------------------------------------------------------------------------------------------
  /**
   * A wrapper for multiRandomSplit that yields a TrainTestHoldoutDataSet
   *
   * @param input           DataSet to be split
   * @param frac            An Tuple3, where the first element specifies the size of the
   *                        training set, the second element the testing set, and the third
   *                        element is the holdout set. These are proportional and will be
   *                        normalized internally.
   * @param precise         Sampling by default is random and can result in slightly lop-sided
   *                        sample sets. When precise is true, equal sample set size are forced,
   *                        however this is somewhat less efficient.
   * @param seed            Random number generator seed.
   * @return A TrainTestDataSet
   */
  def trainTestHoldoutSplit[T: TypeInformation : ClassTag](
      input: DataSet[T],
      frac: Tuple3[Double, Double, Double] = (0.6,0.3,0.1),
      precise: Boolean = false,
      seed: Long = Utils.RNG.nextLong())
    : TrainTestHoldoutDataSet[T] = {
    val fracArray: Array[Double] = frac.productIterator.toArray[Double]
    val dataSetArray = multiRandomSplit(input, fracArray, precise, seed)
    TrainTestHoldoutDataSet(dataSetArray(0), dataSetArray(1), dataSetArray(2))
  }
}
