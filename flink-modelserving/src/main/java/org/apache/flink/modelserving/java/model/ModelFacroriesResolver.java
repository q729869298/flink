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

package org.apache.flink.modelserving.java.model;

import org.apache.flink.annotation.Public;

/**
 * Base interface for ModelFactories resolver. The implementation of this trait should return model factory
 * base on a model type. Currently the following types are defined:
 *         TENSORFLOW  = 0;
 *         TENSORFLOWSAVED  = 1;
 *         PMML = 2;
 * Additional types can be defined as required
 */
@Public
public interface ModelFacroriesResolver {
	/**
	 * Get factory base on factory type.
	 * @param type
	 *            Factory type
	 * @return Model Factory
	 */
	ModelFactory getFactory(int type);
}
