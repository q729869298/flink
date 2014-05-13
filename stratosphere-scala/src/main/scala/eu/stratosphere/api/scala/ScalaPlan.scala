/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.scala

import scala.collection.JavaConversions.asJavaCollection
import eu.stratosphere.api.common.Plan
import eu.stratosphere.compiler.plan.OptimizedPlan
import eu.stratosphere.compiler.postpass.RecordModelPostPass
import java.util.Calendar
import eu.stratosphere.api.common.operators.Operator
import eu.stratosphere.api.scala.analysis.GlobalSchemaGenerator
import eu.stratosphere.api.scala.analysis.postPass.GlobalSchemaOptimizer
import eu.stratosphere.api.common.Program
import eu.stratosphere.api.common.ProgramDescription
import eu.stratosphere.types.Record

class ScalaPlan(scalaSinks: Seq[ScalaSink[_]], scalaJobName: String = "PACT SCALA Job at " + Calendar.getInstance().getTime()) extends Plan(asJavaCollection(scalaSinks map { _.sink }), scalaJobName) {
  val pactSinks = scalaSinks map { _.sink.asInstanceOf[Operator[Record] with ScalaOperator[_, _]] }
  new GlobalSchemaGenerator().initGlobalSchema(pactSinks)
  override def getPostPassClassName() = "eu.stratosphere.api.scala.ScalaPostPass";
}

case class Args(argsMap: Map[String, String], defaultParallelism: Int, schemaHints: Boolean, schemaCompaction: Boolean) {
  def apply(key: String): String = argsMap.getOrElse(key, key)
  def apply(key: String, default: => String) = argsMap.getOrElse(key, default)
}

object Args {

  def parse(args: Seq[String]): Args = {

    var argsMap = Map[String, String]()
    var defaultParallelism = 1
    var schemaHints = true
    var schemaCompaction = true

    val ParamName = "-(.+)".r

    def parse(args: Seq[String]): Unit = args match {
      case Seq("-subtasks", value, rest @ _*)     => { defaultParallelism = value.toInt; parse(rest) }
      case Seq("-nohints", rest @ _*)             => { schemaHints = false; parse(rest) }
      case Seq("-nocompact", rest @ _*)           => { schemaCompaction = false; parse(rest) }
      case Seq(ParamName(name), value, rest @ _*) => { argsMap = argsMap.updated(name, value); parse(rest) }
      case Seq()                                  =>
    }

    parse(args)
    Args(argsMap, defaultParallelism, schemaHints, schemaCompaction)
  }
}

//abstract class ScalaProgram extends Program {
//  def getScalaPlan(args: Args): ScalaPlan
//  
//  override def getPlan(args: String*): Plan = {
//    val scalaArgs = Args.parse(args.toSeq)
//    
//    getScalaPlan(scalaArgs)
//  }
//}


class ScalaPostPass extends RecordModelPostPass with GlobalSchemaOptimizer {
  override def postPass(plan: OptimizedPlan): Unit = {
    optimizeSchema(plan, false)
    super.postPass(plan)
  }
}
