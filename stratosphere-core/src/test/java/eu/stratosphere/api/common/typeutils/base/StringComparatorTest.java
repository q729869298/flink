/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.common.typeutils.base;

import eu.stratosphere.api.common.typeutils.ComparatorTestBase;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;

public class StringComparatorTest extends ComparatorTestBase<String> {

	@Override
	protected TypeComparator<String> createComparator(boolean ascending) {
		return new StringComparator(ascending);
	}

	@Override
	protected TypeSerializer<String> createSerializer() {
		return new StringSerializer();
	}

	@Override
	protected String[] getSortedTestData() {
		return new String[]{
			"",
			"Lorem Ipsum Dolor Omit Longer",
			"aaaa",
			"abcd",
			"abce",
			"abdd",
			"accd",
			"bbcd",
			"bbcde",
			((char)128)+""+((char)32896)
		};
	}
}
