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

package org.apache.flink.examples.modelserving.java.model;

import org.apache.flink.modelserving.java.model.Model;
import org.apache.flink.modelserving.java.model.ModelFactory;
import org.apache.flink.modelserving.java.model.ModelToServe;

import java.util.Optional;

/**
 * Implementation of PMML model factory.
 */
public class WinePMMLModelFactory implements ModelFactory {

	private static ModelFactory instance = null;

	/**
	 * Default constructor - protected.
	 */
	private WinePMMLModelFactory(){}

	/**
	 * Creates a new PMML model.
	 *
	 * @param descriptor model to serve representation of PMML model.
	 * @return model
	 */
	@Override
	public Optional<Model> create(ModelToServe descriptor) {
		try {
			return Optional.of(new WinePMMLModel(descriptor.getModelData()));
		}
		catch (Throwable t){
			System.out.println("Exception creating SpecificPMMLModel from " + descriptor);
			t.printStackTrace();
			return Optional.empty();
		}
	}

	/**
	 * Restore PMML model from binary.
	 *
	 * @param bytes binary representation of PMML model.
	 * @return model
	 */
	@Override
	public Model restore(byte[] bytes) {
		try {
			return new WinePMMLModel(bytes);
		}
		catch (Throwable t){
			System.out.println("Exception restoring SpecificPMMLModel from ");
			t.printStackTrace();
			return null;
		}
	}

	/**
	 * Get model factory instance.
	 *
	 * @return model factory
	 */
	public static ModelFactory getInstance(){
		if (instance == null) {
			instance = new WinePMMLModelFactory();
		}
		return instance;
	}
}
