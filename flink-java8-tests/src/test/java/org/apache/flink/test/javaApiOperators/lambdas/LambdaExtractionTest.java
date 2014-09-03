/**
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

package org.apache.flink.test.javaApiOperators.lambdas;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import junit.framework.Assert;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.types.TypeInformation;
import org.junit.Test;

@SuppressWarnings("serial")
public class LambdaExtractionTest {

	@Test
	public void testIdentifyLambdas() {
		try {
			MapFunction<?, ?> anonymousFromInterface = new MapFunction<String, Integer>() {
				@Override
				public Integer map(String value) { return Integer.parseInt(value); }
			};
			
			MapFunction<?, ?> anonymousFromClass = new RichMapFunction<String, Integer>() {
				@Override
				public Integer map(String value) { return Integer.parseInt(value); }
			};
			
			MapFunction<?, ?> fromProperClass = new StaticMapper();
			
			MapFunction<?, ?> fromDerived = new ToTuple<Integer>() {
				@Override
				public Tuple2<Integer, Long> map(Integer value) {
					return new Tuple2<Integer, Long>(value, 1L);
				}
			};
			
			MapFunction<String, Integer> lambda = (str) -> Integer.parseInt(str);
			
			assertFalse(FunctionUtils.isLambdaFunction(anonymousFromInterface));
			assertFalse(FunctionUtils.isLambdaFunction(anonymousFromClass));
			assertFalse(FunctionUtils.isLambdaFunction(fromProperClass));
			assertFalse(FunctionUtils.isLambdaFunction(fromDerived));
			assertTrue(FunctionUtils.isLambdaFunction(lambda));
			assertTrue(FunctionUtils.isLambdaFunction(STATIC_LAMBDA));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public static class StaticMapper implements MapFunction<String, Integer> {

		@Override
		public Integer map(String value) { return Integer.parseInt(value); }
	}
	
	public interface ToTuple<T> extends MapFunction<T, Tuple2<T, Long>> {

		@Override
		public Tuple2<T, Long> map(T value) throws Exception;
	}
	
	private static final MapFunction<String, Integer> STATIC_LAMBDA = (str) -> Integer.parseInt(str);
	
	public static class MyClass {
		private String s = "mystring";
		
		public MapFunction<Integer, String> getMapFunction() {
			return (i) -> s;
		}
	}
	@Test
	public void testLambdaWithMemberVariable() {		
		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(new MyClass().getMapFunction(), TypeInfoParser.parse("Integer"));
		Assert.assertEquals(ti, BasicTypeInfo.STRING_TYPE_INFO);
	}
	
	@Test
	public void testLambdaWithLocalVariable() {
		String s = "mystring";
		final int k = 24;
		int j = 26;
		
		MapFunction<Integer, String> f = (i) -> s + k + j;
		
		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(f, TypeInfoParser.parse("Integer"));
		Assert.assertEquals(ti, BasicTypeInfo.STRING_TYPE_INFO);
	}
	
	@Test
	public void testMapLambda() {
		MapFunction<Tuple2<Tuple1<Integer>, Boolean>, Tuple2<Tuple1<Integer>, String>> f = (i) -> null;
		
		TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(f, TypeInfoParser.parse("Tuple2<Tuple1<Integer>, Boolean>"));
		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		Assert.assertTrue(((TupleTypeInfo<?>) ti).getTypeAt(0).isTupleType());
		Assert.assertEquals(((TupleTypeInfo<?>) ti).getTypeAt(1), BasicTypeInfo.STRING_TYPE_INFO);
	}
	
	@Test
	public void testFlatMapLambda() {
		FlatMapFunction<Tuple2<Tuple1<Integer>, Boolean>, Tuple2<Tuple1<Integer>, String>> f = (i, o) -> {};
		
		TypeInformation<?> ti = TypeExtractor.getFlatMapReturnTypes(f, TypeInfoParser.parse("Tuple2<Tuple1<Integer>, Boolean>"));
		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		Assert.assertTrue(((TupleTypeInfo<?>) ti).getTypeAt(0).isTupleType());
		Assert.assertEquals(((TupleTypeInfo<?>) ti).getTypeAt(1), BasicTypeInfo.STRING_TYPE_INFO);
	}
	
	@Test
	public void testMapPartitionLambda() {
		MapPartitionFunction<Tuple2<Tuple1<Integer>, Boolean>, Tuple2<Tuple1<Integer>, String>> f = (i, o) -> {};
		
		TypeInformation<?> ti = TypeExtractor.getMapPartitionReturnTypes(f, TypeInfoParser.parse("Tuple2<Tuple1<Integer>, Boolean>"));
		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		Assert.assertTrue(((TupleTypeInfo<?>) ti).getTypeAt(0).isTupleType());
		Assert.assertEquals(((TupleTypeInfo<?>) ti).getTypeAt(1), BasicTypeInfo.STRING_TYPE_INFO);
	}
	
	@Test
	public void testGroupReduceLambda() {
		GroupReduceFunction<Tuple2<Tuple1<Integer>, Boolean>, Tuple2<Tuple1<Integer>, String>> f = (i, o) -> {};
		
		TypeInformation<?> ti = TypeExtractor.getGroupReduceReturnTypes(f, TypeInfoParser.parse("Tuple2<Tuple1<Integer>, Boolean>"));
		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		Assert.assertTrue(((TupleTypeInfo<?>) ti).getTypeAt(0).isTupleType());
		Assert.assertEquals(((TupleTypeInfo<?>) ti).getTypeAt(1), BasicTypeInfo.STRING_TYPE_INFO);
	}
	
	@Test
	public void testFlatJoinLambda() {
		FlatJoinFunction<Tuple2<Tuple1<Integer>, Boolean>, Tuple2<Tuple1<Integer>, Double>, Tuple2<Tuple1<Integer>, String>> f = (i1, i2, o) -> {};
		
		TypeInformation<?> ti = TypeExtractor.getFlatJoinReturnTypes(f, TypeInfoParser.parse("Tuple2<Tuple1<Integer>, Boolean>"), TypeInfoParser.parse("Tuple2<Tuple1<Integer>, Double>"));
		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		Assert.assertTrue(((TupleTypeInfo<?>) ti).getTypeAt(0).isTupleType());
		Assert.assertEquals(((TupleTypeInfo<?>) ti).getTypeAt(1), BasicTypeInfo.STRING_TYPE_INFO);
	}
	
	@Test
	public void testJoinLambda() {
		JoinFunction<Tuple2<Tuple1<Integer>, Boolean>, Tuple2<Tuple1<Integer>, Double>, Tuple2<Tuple1<Integer>, String>> f = (i1, i2) -> null;
		
		TypeInformation<?> ti = TypeExtractor.getJoinReturnTypes(f, TypeInfoParser.parse("Tuple2<Tuple1<Integer>, Boolean>"), TypeInfoParser.parse("Tuple2<Tuple1<Integer>, Double>"));
		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		Assert.assertTrue(((TupleTypeInfo<?>) ti).getTypeAt(0).isTupleType());
		Assert.assertEquals(((TupleTypeInfo<?>) ti).getTypeAt(1), BasicTypeInfo.STRING_TYPE_INFO);
	}
	
	@Test
	public void testCoGroupLambda() {
		CoGroupFunction<Tuple2<Tuple1<Integer>, Boolean>, Tuple2<Tuple1<Integer>, Double>, Tuple2<Tuple1<Integer>, String>> f = (i1, i2, o) -> {};
		
		TypeInformation<?> ti = TypeExtractor.getCoGroupReturnTypes(f, TypeInfoParser.parse("Tuple2<Tuple1<Integer>, Boolean>"), TypeInfoParser.parse("Tuple2<Tuple1<Integer>, Double>"));
		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		Assert.assertTrue(((TupleTypeInfo<?>) ti).getTypeAt(0).isTupleType());
		Assert.assertEquals(((TupleTypeInfo<?>) ti).getTypeAt(1), BasicTypeInfo.STRING_TYPE_INFO);
	}
	
	@Test
	public void testCrossLambda() {
		CrossFunction<Tuple2<Tuple1<Integer>, Boolean>, Tuple2<Tuple1<Integer>, Double>, Tuple2<Tuple1<Integer>, String>> f = (i1, i2) -> null;
		
		TypeInformation<?> ti = TypeExtractor.getCrossReturnTypes(f, TypeInfoParser.parse("Tuple2<Tuple1<Integer>, Boolean>"), TypeInfoParser.parse("Tuple2<Tuple1<Integer>, Double>"));
		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		Assert.assertTrue(((TupleTypeInfo<?>) ti).getTypeAt(0).isTupleType());
		Assert.assertEquals(((TupleTypeInfo<?>) ti).getTypeAt(1), BasicTypeInfo.STRING_TYPE_INFO);
	}
	
	@Test
	public void testKeySelectorLambda() {
		KeySelector<Tuple2<Tuple1<Integer>, Boolean>, Tuple2<Tuple1<Integer>, String>> f = (i) -> null;
		
		TypeInformation<?> ti = TypeExtractor.getKeySelectorTypes(f, TypeInfoParser.parse("Tuple2<Tuple1<Integer>, Boolean>"));
		Assert.assertTrue(ti.isTupleType());
		Assert.assertEquals(2, ti.getArity());
		Assert.assertTrue(((TupleTypeInfo<?>) ti).getTypeAt(0).isTupleType());
		Assert.assertEquals(((TupleTypeInfo<?>) ti).getTypeAt(1), BasicTypeInfo.STRING_TYPE_INFO);
	}
	
}
