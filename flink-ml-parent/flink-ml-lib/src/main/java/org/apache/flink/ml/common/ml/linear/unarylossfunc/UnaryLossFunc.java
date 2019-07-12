/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.ml.linear.unarylossfunc;

import java.io.Serializable;

/**
 * Interface of unary loss function.
 * Each function has 2 inputs(eta and y), we take their difference as the unary variable of the loss function.
 * More information about loss function, please see：
 * 1. https://en.wikipedia.org/wiki/Loss_function
 * 2. https://en.wikipedia.org/wiki/Loss_functions_for_classification
 */
public interface UnaryLossFunc extends Serializable {

	/**
	 * Loss function.
	 *
	 * @param eta eta value.
	 * @param y   label value.
	 * @return loss value.
	 */
	double loss(double eta, double y);

	/**
	 * The derivative of loss function.
	 *
	 * @param eta eta value.
	 * @param y   label value.
	 * @return derivative value.
	 */
	double derivative(double eta, double y);

	/**
	 * The second derivative of the loss function.
	 *
	 * @param eta eta value.
	 * @param y   label value.
	 * @return second derivative value.
	 */
	double secondDerivative(double eta, double y);

}
