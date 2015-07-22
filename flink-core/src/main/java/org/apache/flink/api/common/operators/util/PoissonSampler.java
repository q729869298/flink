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
package org.apache.flink.api.common.operators.util;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.distribution.PoissonDistribution;

import java.util.Iterator;

/**
 * A sampler implementation based on Poisson Distribution. While sample elements with replacement,
 * the picked number of each element follow poisson distribution, so we could use poisson distribution
 * to generate random variables for sample.
 *
 * @param <T> The type of sample.
 * @see <a href="https://en.wikipedia.org/wiki/Poisson_distribution">https://en.wikipedia.org/wiki/Poisson_distribution</a>
 */
public class PoissonSampler<T> extends RandomSampler<T> {
	
	private final PoissonDistribution poissonDistribution;
	private final double fraction;
	
	/**
	 * Create a poisson sampler which would sample elements with replacement.
	 *
	 * @param fraction the expected count of each element.
	 * @param seed     random number generator seed for internal PoissonDistribution.
	 */
	public PoissonSampler(double fraction, long seed) {
		Preconditions.checkArgument(fraction >= 0, "fraction should be positive.");
		this.fraction = fraction;
		this.poissonDistribution = new PoissonDistribution(fraction);
		this.poissonDistribution.reseedRandomGenerator(seed);
	}
	
	/**
	 * Create a poisson sampler which would sample elements with replacement.
	 *
	 * @param fraction the expected count of each element.
	 */
	public PoissonSampler(double fraction) {
		Preconditions.checkArgument(fraction >= 0, "fraction should be non-negative.");
		this.fraction = fraction;
		this.poissonDistribution = new PoissonDistribution(fraction);
	}
	
	/**
	 * Sample the input elements, for each input element, generate its count with poisson distribution random variables generation.
	 *
	 * @param input elements to be sampled.
	 * @return the sampled result which is lazy computed upon input elements.
	 */
	@Override
	public Iterator<T> sample(final Iterator<T> input) {
		if (fraction == 0) {
			return EMPTY_ITERABLE;
		}
		
		return new SampledIterator<T>() {
			T currentElement;
			int currentCount = 0;
			
			@Override
			public boolean hasNext() {
				if (currentElement == null || currentCount == 0) {
					while (input.hasNext()) {
						currentElement = input.next();
						currentCount = poissonDistribution.sample();
						if (currentCount > 0) {
							return true;
						}
					}
					return false;
				}
				return true;
			}
			
			@Override
			public T next() {
				T result = currentElement;
				if (currentCount == 0) {
					currentElement = null;
					return null;
				}
				currentCount--;
				return result;
			}
		};
	}
}
