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

package org.apache.flink.table.descriptors

/**
  * Function Descriptor
  */
class FunctionDescriptor(var name: String) extends Descriptor {

  protected var classDescriptor: Option[ClassTypeDescriptor] = None

  def setClassDescriptor(classDescriptor: ClassTypeDescriptor): FunctionDescriptor = {
    this.classDescriptor = Option(classDescriptor)
    this
  }

  def getDescriptorProperties: DescriptorProperties = {
    val descriptorProperties = new DescriptorProperties()
    addProperties(descriptorProperties)
    descriptorProperties
  }

  override def addProperties(properties: DescriptorProperties) {
    properties.putString(FunctionValidator.FUNCTION_NAME, name)
    classDescriptor.foreach(_.addProperties(properties))
  }

}

object FunctionDescriptor {
  def apply(name: String): FunctionDescriptor = new FunctionDescriptor(name)
}
