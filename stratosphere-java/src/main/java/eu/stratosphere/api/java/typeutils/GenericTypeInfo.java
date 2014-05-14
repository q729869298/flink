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
package eu.stratosphere.api.java.typeutils;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.java.typeutils.runtime.AvroSerializer;
import eu.stratosphere.api.java.typeutils.runtime.GenericTypeComparator;


/**
 *
 */
public class GenericTypeInfo<T> extends TypeInformation<T> implements AtomicType<T> {

	private final Class<T> typeClass;
	
	public GenericTypeInfo(Class<T> typeClass) {
		this.typeClass = typeClass;
	}
	
	@Override
	public boolean isBasicType() {
		return false;
	}


	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public Class<T> getTypeClass() {
		return typeClass;
	}
	
	@Override
	public boolean isKeyType() {
		return Comparable.class.isAssignableFrom(typeClass);
	}

	@Override
	public TypeSerializer<T> createSerializer() {
		return new AvroSerializer<T>(this.typeClass);
	}

	@Override
	public TypeComparator<T> createComparator(boolean sortOrderAscending) {
		if (isKeyType()) {
			@SuppressWarnings("unchecked")
			GenericTypeComparator comparator = new GenericTypeComparator(sortOrderAscending, createSerializer(), this.typeClass);

			return comparator;
		}

		throw new UnsupportedOperationException("Generic types that don't implement java.lang.Comparable cannot be used as keys.");
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return typeClass.hashCode() ^ 0x165667b1;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj.getClass() == GenericTypeInfo.class) {
			return typeClass == ((GenericTypeInfo<?>) obj).typeClass;
		} else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return "GenericType<" + typeClass.getCanonicalName() + ">";
	}
}
