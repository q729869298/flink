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

package org.apache.flink.streaming.api.scala

import org.apache.flink.annotation.{Experimental, Public}
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.datastream.{CoGroupedStreams => JavaCoGroupedStreams}
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

import scala.reflect.ClassTag

/**
 * `CoGroupedStreams` represents two [[DataStream]]s that have been co-grouped.
 * A streaming co-group operation is evaluated over elements in a window.
 *
 * To finalize the co-group operation you also need to specify a [[KeySelector]] for
 * both the first and second input and a [[WindowAssigner]]
 *
 * Note: Right now, the groups are being built in memory so you need to ensure that they don't
 * get too big. Otherwise the JVM might crash.
 *
 * Example:
 *
 * {{{
 * val one: DataStream[(String, Int)]  = ...
 * val two: DataStream[(String, Int)] = ...
 *
 * val result = one.coGroup(two)
 *     .where(new MyFirstKeySelector())
 *     .equalTo(new MyFirstKeySelector())
 *     .window(TumblingTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
 *     .apply(new MyCoGroupFunction())
 * } }}}
 */
@Public
object CoGroupedStreams {

  /**
   * A co-group operation that does not yet have its [[KeySelector]]s defined.
   *
   * @tparam T1 Type of the elements from the first input
   * @tparam T2 Type of the elements from the second input
   */
  class Unspecified[T1, T2](input1: DataStream[T1], input2: DataStream[T2]) {

    /**
     * Specifies a [[KeySelector]] for elements from the first input.
     */
    def where[KEY: TypeInformation](keySelector: T1 => KEY): WithKey[T1, T2, KEY] = {
      val cleanFun = clean(keySelector)
      val keyType = implicitly[TypeInformation[KEY]]
      val javaSelector = new KeySelector[T1, KEY] with ResultTypeQueryable[KEY] {
        def getKey(in: T1) = cleanFun(in)
        override def getProducedType: TypeInformation[KEY] = keyType
      }
      new WithKey[T1, T2, KEY](input1, input2, javaSelector, null, keyType)
    }

    /**
     * Specifies a [[KeySelector]] for elements from the second input.
     */
    def equalTo[KEY: TypeInformation](keySelector: T2 => KEY): WithKey[T1, T2, KEY] = {
      val cleanFun = clean(keySelector)
      val keyType = implicitly[TypeInformation[KEY]]
      val javaSelector = new KeySelector[T2, KEY] with ResultTypeQueryable[KEY] {
        def getKey(in: T2) = cleanFun(in)
        override def getProducedType: TypeInformation[KEY] = keyType
      }
      new WithKey[T1, T2, KEY](input1, input2, null, javaSelector, keyType)
    }

    /**
     * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
     * is not disabled in the [[org.apache.flink.api.common.ExecutionConfig]].
     */
    private[flink] def clean[F <: AnyRef](f: F): F = {
      new StreamExecutionEnvironment(input1.javaStream.getExecutionEnvironment).scalaClean(f)
    }
  }

  /**
   * A co-group operation that has [[KeySelector]]s defined for either both or
   * one input.
   *
   * You need to specify a [[KeySelector]] for both inputs using [[where()]] and [[equalTo()]]
   * before you can proceeed with specifying a [[WindowAssigner]] using [[window()]].
   *
   * @tparam T1 Type of the elements from the first input
   * @tparam T2 Type of the elements from the second input
   * @tparam KEY Type of the key. This must be the same for both inputs
   */
  class WithKey[T1, T2, KEY](
      input1: DataStream[T1],
      input2: DataStream[T2],
      keySelector1: KeySelector[T1, KEY],
      keySelector2: KeySelector[T2, KEY],
      keyType: TypeInformation[KEY]) {

    /**
     * Specifies a [[KeySelector]] for elements from the first input.
     */
    def where(keySelector: T1 => KEY): CoGroupedStreams.WithKey[T1, T2, KEY] = {
      val cleanFun = clean(keySelector)
      val localKeyType = keyType
      val javaSelector = new KeySelector[T1, KEY] with ResultTypeQueryable[KEY] {
        def getKey(in: T1) = cleanFun(in)
        override def getProducedType: TypeInformation[KEY] = localKeyType
      }
      new WithKey[T1, T2, KEY](input1, input2, javaSelector, keySelector2, keyType)
    }

    /**
     * Specifies a [[KeySelector]] for elements from the second input.
     */
    def equalTo(keySelector: T2 => KEY): CoGroupedStreams.WithKey[T1, T2, KEY] = {
      val cleanFun = clean(keySelector)
      val localKeyType = keyType
      val javaSelector = new KeySelector[T2, KEY] with ResultTypeQueryable[KEY] {
        def getKey(in: T2) = cleanFun(in)
        override def getProducedType: TypeInformation[KEY] = localKeyType
      }
      new WithKey[T1, T2, KEY](input1, input2, keySelector1, javaSelector, keyType)
    }

    /**
     * Specifies the window on which the co-group operation works.
     */
    @Experimental
    def window[W <: Window](
        assigner: WindowAssigner[_ >: JavaCoGroupedStreams.TaggedUnion[T1, T2], W])
        : CoGroupedStreams.WithWindow[T1, T2, KEY, W] = {
      if (keySelector1 == null || keySelector2 == null) {
        throw new UnsupportedOperationException("You first need to specify KeySelectors for both" +
          "inputs using where() and equalTo().")
      }
      new CoGroupedStreams.WithWindow[T1, T2, KEY, W](
        input1,
        input2,
        keySelector1,
        keySelector2,
        clean(assigner),
        null,
        null)
    }

    /**
     * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
     * is not disabled in the [[org.apache.flink.api.common.ExecutionConfig]].
     */
    private[flink] def clean[F <: AnyRef](f: F): F = {
      new StreamExecutionEnvironment(input1.javaStream.getExecutionEnvironment).scalaClean(f)
    }
  }

  /**
   * A co-group operation that has [[KeySelector]]s defined for both inputs as
   * well as a [[WindowAssigner]].
   *
   * @tparam T1 Type of the elements from the first input
   * @tparam T2 Type of the elements from the second input
   * @tparam KEY Type of the key. This must be the same for both inputs
   * @tparam W Type of { @link Window} on which the co-group operation works.
   */
  @Experimental
  class WithWindow[T1, T2, KEY, W <: Window](
      input1: DataStream[T1],
      input2: DataStream[T2],
      keySelector1: KeySelector[T1, KEY],
      keySelector2: KeySelector[T2, KEY],
      windowAssigner: WindowAssigner[_ >: JavaCoGroupedStreams.TaggedUnion[T1, T2], W],
      trigger: Trigger[_ >: JavaCoGroupedStreams.TaggedUnion[T1, T2], _ >: W],
      evictor: Evictor[_ >: JavaCoGroupedStreams.TaggedUnion[T1, T2], _ >: W]) {


    /**
     * Sets the [[Trigger]] that should be used to trigger window emission.
     */
    @Experimental
    def trigger(newTrigger: Trigger[_ >: JavaCoGroupedStreams.TaggedUnion[T1, T2], _ >: W])
    : CoGroupedStreams.WithWindow[T1, T2, KEY, W] = {
      new WithWindow[T1, T2, KEY, W](
        input1,
        input2,
        keySelector1,
        keySelector2,
        windowAssigner,
        newTrigger,
        evictor)
    }

    /**
     * Sets the [[Evictor]] that should be used to evict elements from a window before emission.
     *
     * Note: When using an evictor window performance will degrade significantly, since
     * pre-aggregation of window results cannot be used.
     */
    @Experimental
    def evictor(newEvictor: Evictor[_ >: JavaCoGroupedStreams.TaggedUnion[T1, T2], _ >: W])
    : CoGroupedStreams.WithWindow[T1, T2, KEY, W] = {
      new WithWindow[T1, T2, KEY, W](
        input1,
        input2,
        keySelector1,
        keySelector2,
        windowAssigner,
        trigger,
        newEvictor)
    }

    /**
     * Completes the co-group operation with the user function that is executed
     * for windowed groups.
     */
    def apply[O: TypeInformation: ClassTag](
        fun: (Iterator[T1], Iterator[T2]) => O): DataStream[O] = {
      require(fun != null, "CoGroup function must not be null.")

      val coGrouper = new CoGroupFunction[T1, T2, O] {
        val cleanFun = clean(fun)
        def coGroup(
            left: java.lang.Iterable[T1],
            right: java.lang.Iterable[T2], out: Collector[O]) = {
          out.collect(cleanFun(left.iterator().asScala, right.iterator().asScala))
        }
      }
      apply(coGrouper)
    }

    /**
     * Completes the co-group operation with the user function that is executed
     * for windowed groups.
     */
    def apply[O: TypeInformation: ClassTag](
        fun: (Iterator[T1], Iterator[T2], Collector[O]) => Unit): DataStream[O] = {
      require(fun != null, "CoGroup function must not be null.")

      val coGrouper = new CoGroupFunction[T1, T2, O] {
        val cleanFun = clean(fun)
        def coGroup(
            left: java.lang.Iterable[T1],
            right: java.lang.Iterable[T2], out: Collector[O]) = {
          cleanFun(left.iterator.asScala, right.iterator.asScala, out)
        }
      }
      apply(coGrouper)
    }

    /**
     * Completes the co-group operation with the user function that is executed
     * for windowed groups.
     */
    def apply[T: TypeInformation](function: CoGroupFunction[T1, T2, T]): DataStream[T] = {

      val coGroup = new JavaCoGroupedStreams[T1, T2](input1.javaStream, input2.javaStream)

      coGroup
        .where(keySelector1)
        .equalTo(keySelector2)
        .window(windowAssigner)
        .trigger(trigger)
        .evictor(evictor)
        .apply(clean(function), implicitly[TypeInformation[T]])
    }

    /**
     * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
     * is not disabled in the [[org.apache.flink.api.common.ExecutionConfig]].
     */
    private[flink] def clean[F <: AnyRef](f: F): F = {
      new StreamExecutionEnvironment(input1.javaStream.getExecutionEnvironment).scalaClean(f)
    }
  }


  /**
   * Creates a new co-group operation from the two given inputs.
   */
  def createCoGroup[T1, T2](input1: DataStream[T1], input2: DataStream[T2])
      : CoGroupedStreams.Unspecified[T1, T2] = {
    new CoGroupedStreams.Unspecified[T1, T2](input1, input2)
  }

}

