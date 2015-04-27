/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.source.GenericSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

public abstract class ConnectorSource<OUT> extends RichParallelSourceFunction<OUT> implements
		GenericSourceFunction<OUT> {

	private static final long serialVersionUID = 1L;
	protected DeserializationSchema<OUT> schema;

	public ConnectorSource(DeserializationSchema<OUT> schema) {
		this.schema = schema;
	}

	@Override
	public TypeInformation<OUT> getType() {
		if(schema instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<OUT>) schema).getProducedType();
		}
		return TypeExtractor.createTypeInfo(DeserializationSchema.class, schema.getClass(), 0,
				null, null);
	}

}
