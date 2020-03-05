/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.SortedMap;
import java.util.stream.Collectors;


/**
 * Tests for {@link IntervalJoinOperator}.
 * Those tests cover correctness and cleaning of state
 */
@RunWith(Parameterized.class)
public class IntervalJoinOperatorTest {

	private final boolean lhsFasterThanRhs;

	@Parameters(name = "lhs faster than rhs: {0}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
			{true}, {false}
		});
	}

	public IntervalJoinOperatorTest(boolean lhsFasterThanRhs) {
		this.lhsFasterThanRhs = lhsFasterThanRhs;
	}

	@Test
	public void testImplementationMirrorsCorrectly() throws Exception {

		long lowerBound = 1;
		long upperBound = 3;

		boolean lowerBoundInclusive = true;
		boolean upperBoundInclusive = false;

		setupHarness(lowerBound, lowerBoundInclusive, upperBound, upperBoundInclusive, new PassthroughFunction.JoinParameters())
			.processElementsAndWatermarks(1, 4)
			.andExpect(
				streamRecordOf(1, 2),
				streamRecordOf(1, 3),
				streamRecordOf(2, 3),
				streamRecordOf(2, 4),
				streamRecordOf(3, 4))
			.noLateRecords()
			.close();

		setupHarness(-1 * upperBound, upperBoundInclusive, -1 * lowerBound, lowerBoundInclusive, new PassthroughFunction.JoinParameters())
			.processElementsAndWatermarks(1, 4)
			.andExpect(
				streamRecordOf(2, 1),
				streamRecordOf(3, 1),
				streamRecordOf(3, 2),
				streamRecordOf(4, 2),
				streamRecordOf(4, 3))
			.noLateRecords()
			.close();
	}

	@Test // lhs - 2 <= rhs <= rhs + 2
	public void testNegativeInclusiveAndNegativeInclusive() throws Exception {

		setupHarness(-2, true, -1, true, new PassthroughFunction.JoinParameters())
			.processElementsAndWatermarks(1, 4)
			.andExpect(
				streamRecordOf(2, 1),
				streamRecordOf(3, 1),
				streamRecordOf(3, 2),
				streamRecordOf(4, 2),
				streamRecordOf(4, 3)
			)
			.noLateRecords()
			.close();
	}

	@Test // lhs - 1 <= rhs <= rhs + 1
	public void testNegativeInclusiveAndPositiveInclusive() throws Exception {

		setupHarness(-1, true, 1, true, new PassthroughFunction.JoinParameters())
			.processElementsAndWatermarks(1, 4)
			.andExpect(
				streamRecordOf(1, 1),
				streamRecordOf(1, 2),
				streamRecordOf(2, 1),
				streamRecordOf(2, 2),
				streamRecordOf(2, 3),
				streamRecordOf(3, 2),
				streamRecordOf(3, 3),
				streamRecordOf(3, 4),
				streamRecordOf(4, 3),
				streamRecordOf(4, 4)
			)
			.noLateRecords()
			.close();
	}

	@Test // lhs + 1 <= rhs <= lhs + 2
	public void testPositiveInclusiveAndPositiveInclusive() throws Exception {

		setupHarness(1, true, 2, true, new PassthroughFunction.JoinParameters())
			.processElementsAndWatermarks(1, 4)
			.andExpect(
				streamRecordOf(1, 2),
				streamRecordOf(1, 3),
				streamRecordOf(2, 3),
				streamRecordOf(2, 4),
				streamRecordOf(3, 4)
			)
			.noLateRecords()
			.close();
	}

	@Test
	public void testNegativeExclusiveAndNegativeExlusive() throws Exception {

		setupHarness(-3, false, -1, false, new PassthroughFunction.JoinParameters())
			.processElementsAndWatermarks(1, 4)
			.andExpect(
				streamRecordOf(3, 1),
				streamRecordOf(4, 2)
			)
			.noLateRecords()
			.close();
	}

	@Test
	public void testNegativeExclusiveAndPositiveExlusive() throws Exception {

		setupHarness(-1, false, 1, false, new PassthroughFunction.JoinParameters())
			.processElementsAndWatermarks(1, 4)
			.andExpect(
				streamRecordOf(1, 1),
				streamRecordOf(2, 2),
				streamRecordOf(3, 3),
				streamRecordOf(4, 4)
			)
			.noLateRecords()
			.close();
	}

	@Test
	public void testPositiveExclusiveAndPositiveExlusive() throws Exception {

		setupHarness(1, false, 3, false, new PassthroughFunction.JoinParameters())
			.processElementsAndWatermarks(1, 4)
			.andExpect(
				streamRecordOf(1, 3),
				streamRecordOf(2, 4)
			)
			.noLateRecords()
			.close();
	}

	@Test
	public void testStateCleanupNegativeInclusiveNegativeInclusive() throws Exception {

		setupHarness(-1, true, 0, true, new PassthroughFunction.JoinParameters())
			.processElement1(1)
			.processElement1(2)
			.processElement1(3)
			.processElement1(4)
			.processElement1(5)

			.processElement2(1)
			.processElement2(2)
			.processElement2(3)
			.processElement2(4)
			.processElement2(5) // fill both buffers with values

			.processWatermark1(1)
			.processWatermark2(1) // set common watermark to 1 and check that data is cleaned

			.assertLeftBufferContainsOnly(2, 3, 4, 5)
			.assertRightBufferContainsOnly(1, 2, 3, 4, 5)

			.processWatermark1(4) // set common watermark to 4 and check that data is cleaned
			.processWatermark2(4)

			.assertLeftBufferContainsOnly(5)
			.assertRightBufferContainsOnly(4, 5)

			.processWatermark1(6) // set common watermark to 6 and check that data all buffers are empty
			.processWatermark2(6)

			.assertLeftBufferEmpty()
			.assertRightBufferEmpty()

			.close();
	}

	@Test
	public void testStateCleanupNegativePositiveNegativeExlusive() throws Exception {
		setupHarness(-2, false, 1, false, new PassthroughFunction.JoinParameters())
			.processElement1(1)
			.processElement1(2)
			.processElement1(3)
			.processElement1(4)
			.processElement1(5)

			.processElement2(1)
			.processElement2(2)
			.processElement2(3)
			.processElement2(4)
			.processElement2(5) // fill both buffers with values

			.processWatermark1(1)
			.processWatermark2(1) // set common watermark to 1 and check that data is cleaned

			.assertLeftBufferContainsOnly(2, 3, 4, 5)
			.assertRightBufferContainsOnly(1, 2, 3, 4, 5)

			.processWatermark1(4) // set common watermark to 4 and check that data is cleaned
			.processWatermark2(4)

			.assertLeftBufferContainsOnly(5)
			.assertRightBufferContainsOnly(4, 5)

			.processWatermark1(6) // set common watermark to 6 and check that data all buffers are empty
			.processWatermark2(6)

			.assertLeftBufferEmpty()
			.assertRightBufferEmpty()

			.close();
	}

	@Test
	public void testStateCleanupPositiveInclusivePositiveInclusive() throws Exception {
		setupHarness(0, true, 1, true, new PassthroughFunction.JoinParameters())
			.processElement1(1)
			.processElement1(2)
			.processElement1(3)
			.processElement1(4)
			.processElement1(5)

			.processElement2(1)
			.processElement2(2)
			.processElement2(3)
			.processElement2(4)
			.processElement2(5) // fill both buffers with values

			.processWatermark1(1)
			.processWatermark2(1) // set common watermark to 1 and check that data is cleaned

			.assertLeftBufferContainsOnly(1, 2, 3, 4, 5)
			.assertRightBufferContainsOnly(2, 3, 4, 5)

			.processWatermark1(4) // set common watermark to 4 and check that data is cleaned
			.processWatermark2(4)

			.assertLeftBufferContainsOnly(4, 5)
			.assertRightBufferContainsOnly(5)

			.processWatermark1(6) // set common watermark to 6 and check that data all buffers are empty
			.processWatermark2(6)

			.assertLeftBufferEmpty()
			.assertRightBufferEmpty()

			.close();
	}

	@Test
	public void testStateCleanupPositiveExlusivePositiveExclusive() throws Exception {
		setupHarness(-1, false, 2, false, new PassthroughFunction.JoinParameters())
			.processElement1(1)
			.processElement1(2)
			.processElement1(3)
			.processElement1(4)
			.processElement1(5)

			.processElement2(1)
			.processElement2(2)
			.processElement2(3)
			.processElement2(4)
			.processElement2(5) // fill both buffers with values

			.processWatermark1(1)
			.processWatermark2(1) // set common watermark to 1 and check that data is cleaned

			.assertLeftBufferContainsOnly(1, 2, 3, 4, 5)
			.assertRightBufferContainsOnly(2, 3, 4, 5)

			.processWatermark1(4) // set common watermark to 4 and check that data is cleaned
			.processWatermark2(4)

			.assertLeftBufferContainsOnly(4, 5)
			.assertRightBufferContainsOnly(5)

			.processWatermark1(6) // set common watermark to 6 and check that data all buffers are empty
			.processWatermark2(6)

			.assertLeftBufferEmpty()
			.assertRightBufferEmpty()

			.close();
	}

	@Test
	public void testOneSideCacheFlipSides() throws Exception {
		setupHarness(0, true, 0, true,
			new PassthroughFunction.JoinParameters(Long.MAX_VALUE, 1, Long.MAX_VALUE))
			.processElement2(1)

			.assertOneSideCacheContainsOnly() // empty other side buffer

			.processElement1(2)

			.assertOneSideCacheContainsOnly(1) // cache right side buffer

			.processElement2(3)

			.assertOneSideCacheContainsOnly(2) // cache left side buffer

			.assertLeftBufferContainsOnly(2)
			.assertRightBufferContainsOnly(1, 3)

			.processWatermark1(2)
			.assertLeftBufferContainsOnly(2)
			.assertOneSideCacheContainsOnly(2) // wait for low watermark

			.processWatermark2(2)
			.assertLeftBufferContainsOnly()
			.assertOneSideCacheContainsOnly() // cache is synced with left buffer

			.processWatermark1(3)
			.processWatermark2(3)

			.assertLeftBufferContainsOnly()
			.assertRightBufferContainsOnly()
			.assertOneSideCacheContainsOnly()

			.close();
	}

	@Test
	public void testOneSideCacheExpiration() throws Exception {
		setupHarness(-1, false, 2, false,
			new PassthroughFunction.JoinParameters(Long.MAX_VALUE, 1, 999))
			.processElement2(1)
			.processElement2(2)
			.processElement2(3)
			.processElement2(4)
			.processElement2(5) // fill both buffers with values

			.processWatermark1(0)
			.processWatermark2(0)

			.assertOneSideCacheContainsOnly()

			.processElement1(1)

			.assertRightBufferContainsOnly(1, 2, 3, 4, 5)
			.assertOneSideCacheContainsOnly(1, 2, 3, 4, 5) // same as right buffer
			.processWatermark1(1)
			.processWatermark2(1)

			.processElement1(1) // access at time 1

			.processWatermark1(2)
			.processWatermark2(2) // time 2

			.assertOneSideCacheSize(0L)

			.processElement1(2)
			.processElement1(3)
			.processElement1(4)
			.processElement1(5)

			.assertLeftBufferContainsOnly(2, 3, 4, 5)
			.assertRightBufferContainsOnly(3, 4, 5)
			.assertOneSideCacheContainsOnly(3, 4, 5)

			.processWatermark1(3)
			.processWatermark2(3)

			.assertLeftBufferContainsOnly(3, 4, 5)
			.assertRightBufferContainsOnly(4, 5)
			.assertOneSideCacheSize(0L)

			.close();
	}

	@Test
	public void testRightSideSKipWriteBuffer() throws Exception {
		setupHarness(-1, false, 2, false,
			new PassthroughFunction.JoinParameters(-1, 1, Long.MAX_VALUE))
			.processElement2(1)
			.processElement2(2)
			.processElement2(3)
			.processElement2(4)
			.processElement2(5) // fill both buffers with values

			.assertOneSideCacheContainsOnly()

			.processElement1(1)
			.assertLeftBufferContainsOnly(1)
			.assertRightBufferContainsOnly()
			.assertOneSideCacheContainsOnly() // same as right buffer

			.processWatermark1(1)
			.processWatermark2(1) // set common watermark to 1 and check that data is cleaned

			.processElement1(2)
			.processElement2(3)

			.assertLeftBufferContainsOnly(1, 2)
			.assertRightBufferContainsOnly()
			.assertOneSideCacheContainsOnly(1, 2) // same as other side buffer after early cleanup

			.processWatermark1(4) // set common watermark to 6 and check that data all buffers are empty
			.processWatermark2(4)

			.assertLeftBufferEmpty()
			.assertRightBufferEmpty()
			.assertOneSideCacheContainsOnly()

			.close();
	}

	@Test
	public void testRightSideCleanupOverwrite() throws Exception {
		setupHarness(-1, false, 2, false,
			new PassthroughFunction.JoinParameters(0, 1, Long.MAX_VALUE))
			.processElement2(1)
			.processElement2(2)
			.processElement2(3)
			.processElement2(4)
			.processElement2(5) // fill both buffers with values

			.assertOneSideCacheContainsOnly()

			.processElement1(1)

			.assertRightBufferContainsOnly(1, 2, 3, 4, 5)
			.assertOneSideCacheContainsOnly(1, 2, 3, 4, 5) // same as right buffer

			.processWatermark1(1)
			.processWatermark2(1) // set common watermark to 1 and check that data is cleaned

			.processElement1(2)
			.processElement1(3)
			.processElement1(4)
			.processElement1(5)

			.assertLeftBufferContainsOnly(1, 2, 3, 4, 5)
			.assertRightBufferContainsOnly(3, 4, 5)
			.assertOneSideCacheContainsOnly(3, 4, 5) // same as other side buffer after early cleanup

			.processWatermark1(4) // set common watermark to 4 and check that data is cleaned
			.processWatermark2(4)

			.assertLeftBufferContainsOnly(4, 5)
			.assertRightBufferContainsOnly()
			.assertOneSideCacheContainsOnly() // watermark doesn't flip cache side

			.processWatermark1(6) // set common watermark to 6 and check that data all buffers are empty
			.processWatermark2(6)

			.assertLeftBufferEmpty()
			.assertRightBufferEmpty()
			.assertOneSideCacheContainsOnly()

			.close();
	}

	@Test
	public void testRestoreFromSnapshot() throws Exception {

		// config
		int lowerBound = -1;
		boolean lowerBoundInclusive = true;
		int upperBound = 1;
		boolean upperBoundInclusive = true;

		// create first test harness
		OperatorSubtaskState handles;
		List<StreamRecord<Tuple2<TestElem, TestElem>>> expectedOutput;

		try (TestHarness testHarness = createTestHarness(
			lowerBound,
			lowerBoundInclusive,
			upperBound,
			upperBoundInclusive
		)) {

			testHarness.setup();
			testHarness.open();

			// process elements with first test harness
			testHarness.processElement1(createStreamRecord(1, "lhs"));
			testHarness.processWatermark1(new Watermark(1));

			testHarness.processElement2(createStreamRecord(1, "rhs"));
			testHarness.processWatermark2(new Watermark(1));

			testHarness.processElement1(createStreamRecord(2, "lhs"));
			testHarness.processWatermark1(new Watermark(2));

			testHarness.processElement2(createStreamRecord(2, "rhs"));
			testHarness.processWatermark2(new Watermark(2));

			testHarness.processElement1(createStreamRecord(3, "lhs"));
			testHarness.processWatermark1(new Watermark(3));

			testHarness.processElement2(createStreamRecord(3, "rhs"));
			testHarness.processWatermark2(new Watermark(3));

			// snapshot and validate output
			handles = testHarness.snapshot(0, 0);
			testHarness.close();

			expectedOutput = Lists.newArrayList(
				streamRecordOf(1, 1),
				streamRecordOf(1, 2),
				streamRecordOf(2, 1),
				streamRecordOf(2, 2),
				streamRecordOf(2, 3),
				streamRecordOf(3, 2),
				streamRecordOf(3, 3)
			);

			TestHarnessUtil.assertNoLateRecords(testHarness.getOutput());
			assertOutput(expectedOutput, testHarness.getOutput());
		}

		try (TestHarness newTestHarness = createTestHarness(
			lowerBound,
			lowerBoundInclusive,
			upperBound,
			upperBoundInclusive
		)) {
			// create new test harness from snapshpt

			newTestHarness.setup();
			newTestHarness.initializeState(handles);
			newTestHarness.open();

			// process elements
			newTestHarness.processElement1(createStreamRecord(4, "lhs"));
			newTestHarness.processWatermark1(new Watermark(4));

			newTestHarness.processElement2(createStreamRecord(4, "rhs"));
			newTestHarness.processWatermark2(new Watermark(4));

			// assert expected output
			expectedOutput = Lists.newArrayList(
				streamRecordOf(3, 4),
				streamRecordOf(4, 3),
				streamRecordOf(4, 4)
			);

			TestHarnessUtil.assertNoLateRecords(newTestHarness.getOutput());
			assertOutput(expectedOutput, newTestHarness.getOutput());
		}
	}

	@Test
	public void testContextCorrectLeftTimestamp() throws Exception {

		IntervalJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> op =
			new IntervalJoinOperator<>(
				-1,
				1,
				true,
				true,
				TestElem.serializer(),
				TestElem.serializer(),
				new ProcessJoinFunction<TestElem, TestElem, Tuple2<TestElem, TestElem>>() {
					@Override
					public void processElement(
						TestElem left,
						TestElem right,
						Context ctx,
						Collector<Tuple2<TestElem, TestElem>> out) throws Exception {
						Assert.assertEquals(left.ts, ctx.getLeftTimestamp());
					}
				}
			);

		try (TestHarness testHarness = new TestHarness(
			op,
			(elem) -> elem.key,
			(elem) -> elem.key,
			TypeInformation.of(String.class)
		)) {

			testHarness.setup();
			testHarness.open();

			processElementsAndWatermarks(testHarness);
		}
	}

	@Test
	public void testReturnsCorrectTimestamp() throws Exception {
		IntervalJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> op =
			new IntervalJoinOperator<>(
				-1,
				1,
				true,
				true,
				TestElem.serializer(),
				TestElem.serializer(),
				new ProcessJoinFunction<TestElem, TestElem, Tuple2<TestElem, TestElem>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void processElement(
						TestElem left,
						TestElem right,
						Context ctx,
						Collector<Tuple2<TestElem, TestElem>> out) throws Exception {
						Assert.assertEquals(Math.max(left.ts, right.ts), ctx.getTimestamp());
					}
				}
			);

		try (TestHarness testHarness = new TestHarness(
			op,
			(elem) -> elem.key,
			(elem) -> elem.key,
			TypeInformation.of(String.class)
		)) {

			testHarness.setup();
			testHarness.open();

			processElementsAndWatermarks(testHarness);
		}
	}

	@Test
	public void testContextCorrectRightTimestamp() throws Exception {

		IntervalJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> op =
			new IntervalJoinOperator<>(
				-1,
				1,
				true,
				true,
				TestElem.serializer(),
				TestElem.serializer(),
				new ProcessJoinFunction<TestElem, TestElem, Tuple2<TestElem, TestElem>>() {
					@Override
					public void processElement(
						TestElem left,
						TestElem right,
						Context ctx,
						Collector<Tuple2<TestElem, TestElem>> out) throws Exception {
						Assert.assertEquals(right.ts, ctx.getRightTimestamp());
					}
				}
			);

		try (TestHarness testHarness = new TestHarness(
			op,
			(elem) -> elem.key,
			(elem) -> elem.key,
			TypeInformation.of(String.class)
		)) {

			testHarness.setup();
			testHarness.open();

			processElementsAndWatermarks(testHarness);
		}
	}

	@Test(expected = FlinkException.class)
	public void testFailsWithNoTimestampsLeft() throws Exception {
		TestHarness newTestHarness = createTestHarness(0L, true, 0L, true);

		newTestHarness.setup();
		newTestHarness.open();

		// note that the StreamRecord has no timestamp in constructor
		newTestHarness.processElement1(new StreamRecord<>(new TestElem(0, "lhs")));
	}

	@Test(expected = FlinkException.class)
	public void testFailsWithNoTimestampsRight() throws Exception {
		try (TestHarness newTestHarness = createTestHarness(0L, true, 0L, true)) {

			newTestHarness.setup();
			newTestHarness.open();

			// note that the StreamRecord has no timestamp in constructor
			newTestHarness.processElement2(new StreamRecord<>(new TestElem(0, "rhs")));
		}
	}

	@Test
	public void testDiscardsLateData() throws Exception {
		setupHarness(-1, true, 1, true, new PassthroughFunction.JoinParameters())
			.processElement1(1)
			.processElement2(1)
			.processElement1(2)
			.processElement2(2)
			.processElement1(3)
			.processElement2(3)
			.processWatermark1(3)
			.processWatermark2(3)
			.processElement1(1) // this element is late and should not be joined again
			.processElement1(4)
			.processElement2(4)
			.processElement1(5)
			.processElement2(5)
			.andExpect(
				streamRecordOf(1, 1),
				streamRecordOf(1, 2),

				streamRecordOf(2, 1),
				streamRecordOf(2, 2),
				streamRecordOf(2, 3),

				streamRecordOf(3, 2),
				streamRecordOf(3, 3),
				streamRecordOf(3, 4),

				streamRecordOf(4, 3),
				streamRecordOf(4, 4),
				streamRecordOf(4, 5),

				streamRecordOf(5, 4),
				streamRecordOf(5, 5)
			)
			.noLateRecords()
			.close();
	}

	private void assertEmpty(MapState<Long, ?> state) throws Exception {
		boolean stateIsEmpty = Iterables.size(state.keys()) == 0;
		Assert.assertTrue("state not empty", stateIsEmpty);
	}

	private void assertContainsOnly(SortedMap<Long, ?> cache, long... ts) throws Exception {
		for (long t : ts) {
			String message = "Keys not found in cache. \n Expected: " + Arrays.toString(ts) + "\n Actual:   " + cache.keySet();
			Assert.assertTrue(message, cache.keySet().contains(t));
		}

		String message = "Too many objects in state. \n Expected: " + Arrays.toString(ts) + "\n Actual:   " + cache.keySet();
		Assert.assertEquals(message, ts.length, Iterables.size(cache.keySet()));
	}

	private void assertContainsOnly(MapState<Long, ?> state, long... ts) throws Exception {
		for (long t : ts) {
			String message = "Keys not found in state. \n Expected: " + Arrays.toString(ts) + "\n Actual:   " + state.keys();
			Assert.assertTrue(message, state.contains(t));
		}

		String message = "Too many objects in state. \n Expected: " + Arrays.toString(ts) + "\n Actual:   " + state.keys();
		Assert.assertEquals(message, ts.length, Iterables.size(state.keys()));
	}

	private void assertOutput(
		Iterable<StreamRecord<Tuple2<TestElem, TestElem>>> expectedOutput,
		Queue<Object> actualOutput) {

		int actualSize = actualOutput.stream()
			.filter(elem -> elem instanceof StreamRecord)
			.collect(Collectors.toList())
			.size();

		int expectedSize = Iterables.size(expectedOutput);

		Assert.assertEquals(
			"Expected and actual size of stream records different",
			expectedSize,
			actualSize
		);

		for (StreamRecord<Tuple2<TestElem, TestElem>> record : expectedOutput) {
			Assert.assertTrue(actualOutput.contains(record));
		}
	}

	private TestHarness createTestHarness(long lowerBound,
		boolean lowerBoundInclusive,
		long upperBound,
		boolean upperBoundInclusive) throws Exception {

		IntervalJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> operator =
			new IntervalJoinOperator<>(
				lowerBound,
				upperBound,
				lowerBoundInclusive,
				upperBoundInclusive,
				TestElem.serializer(),
				TestElem.serializer(),
				new PassthroughFunction()
			);

		return new TestHarness(
			operator,
			(elem) -> elem.key, // key
			(elem) -> elem.key, // key
			TypeInformation.of(String.class)
		);
	}

	private JoinTestBuilder setupHarness(long lowerBound,
		boolean lowerBoundInclusive,
		long upperBound,
		boolean upperBoundInclusive,
		ProcessJoinFunction.JoinParameters params) throws Exception {

		IntervalJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> operator =
			new IntervalJoinOperator<>(
				lowerBound,
				upperBound,
				lowerBoundInclusive,
				upperBoundInclusive,
				TestElem.serializer(),
				TestElem.serializer(),
				new PassthroughFunction(params)
			);

		TestHarness t = new TestHarness(
			operator,
			(elem) -> elem.key, // key
			(elem) -> elem.key, // key
			TypeInformation.of(String.class)
		);

		return new JoinTestBuilder(t, operator);
	}

	private class JoinTestBuilder {

		private IntervalJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> operator;
		private TestHarness testHarness;

		public JoinTestBuilder(
			TestHarness t,
			IntervalJoinOperator<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> operator
		) throws Exception {

			this.testHarness = t;
			this.operator = operator;
			t.open();
			t.setup();
		}

		public TestHarness get() {
			return testHarness;
		}

		public JoinTestBuilder processElement1(int ts) throws Exception {
			testHarness.processElement1(createStreamRecord(ts, "lhs"));
			return this;
		}

		public JoinTestBuilder processElement2(int ts) throws Exception {
			testHarness.processElement2(createStreamRecord(ts, "rhs"));
			return this;
		}

		public JoinTestBuilder processWatermark1(int ts) throws Exception {
			testHarness.processWatermark1(new Watermark(ts));
			return this;
		}

		public JoinTestBuilder processWatermark2(int ts) throws Exception {
			testHarness.processWatermark2(new Watermark(ts));
			return this;
		}

		public JoinTestBuilder processElementsAndWatermarks(int from, int to) throws Exception {
			if (lhsFasterThanRhs) {
				// add to lhs
				for (int i = from; i <= to; i++) {
					testHarness.processElement1(createStreamRecord(i, "lhs"));
					testHarness.processWatermark1(new Watermark(i));
				}

				// add to rhs
				for (int i = from; i <= to; i++) {
					testHarness.processElement2(createStreamRecord(i, "rhs"));
					testHarness.processWatermark2(new Watermark(i));
				}
			} else {
				// add to rhs
				for (int i = from; i <= to; i++) {
					testHarness.processElement2(createStreamRecord(i, "rhs"));
					testHarness.processWatermark2(new Watermark(i));
				}

				// add to lhs
				for (int i = from; i <= to; i++) {
					testHarness.processElement1(createStreamRecord(i, "lhs"));
					testHarness.processWatermark1(new Watermark(i));
				}
			}

			return this;
		}

		@SafeVarargs
		public final JoinTestBuilder andExpect(StreamRecord<Tuple2<TestElem, TestElem>>... elems) {
			assertOutput(Lists.newArrayList(elems), testHarness.getOutput());
			return this;
		}

		public JoinTestBuilder assertLeftBufferContainsOnly(long... timestamps) {

			try {
				assertContainsOnly(operator.getLeftBuffer(), timestamps);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			return this;
		}

		public JoinTestBuilder assertOneSideCacheSize(long size) {
			Assert.assertEquals(operator.getOtherSideCache().size(), size);
			return this;
		}

		public JoinTestBuilder assertOneSideCacheContainsOnly(long... timestamps) {
			try {
				assertContainsOnly(operator.getOtherSideCache().getIfPresent(operator.getCurrentKey()), timestamps);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			return this;
		}

		public JoinTestBuilder assertRightBufferContainsOnly(long... timestamps) {

			try {
				assertContainsOnly(operator.getRightBuffer(), timestamps);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			return this;
		}

		public JoinTestBuilder assertLeftBufferEmpty() {
			try {
				assertEmpty(operator.getLeftBuffer());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			return this;
		}

		public JoinTestBuilder assertRightBufferEmpty() {
			try {
				assertEmpty(operator.getRightBuffer());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			return this;
		}

		public JoinTestBuilder noLateRecords() {
			TestHarnessUtil.assertNoLateRecords(this.testHarness.getOutput());
			return this;
		}

		public void close() throws Exception {
			testHarness.close();
		}
	}

	private static class PassthroughFunction extends ProcessJoinFunction<TestElem, TestElem, Tuple2<TestElem, TestElem>> {

		public PassthroughFunction() {
			super();
		}

		public PassthroughFunction(JoinParameters parameters) {
			this.joinParameters = parameters;
		}

		@Override
		public void processElement(
			TestElem left,
			TestElem right,
			Context ctx,
			Collector<Tuple2<TestElem, TestElem>> out) throws Exception {
			out.collect(Tuple2.of(left, right));
		}
	}

	private StreamRecord<Tuple2<TestElem, TestElem>> streamRecordOf(
		long lhsTs,
		long rhsTs
	) {
		TestElem lhs = new TestElem(lhsTs, "lhs");
		TestElem rhs = new TestElem(rhsTs, "rhs");

		long ts = Math.max(lhsTs, rhsTs);
		return new StreamRecord<>(Tuple2.of(lhs, rhs), ts);
	}

	private static class TestElem {
		String key;
		long ts;
		String source;

		public TestElem(long ts, String source) {
			this.key = "key";
			this.ts = ts;
			this.source = source;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			TestElem testElem = (TestElem) o;

			if (ts != testElem.ts) {
				return false;
			}

			if (key != null ? !key.equals(testElem.key) : testElem.key != null) {
				return false;
			}

			return source != null ? source.equals(testElem.source) : testElem.source == null;
		}

		@Override
		public int hashCode() {
			int result = key != null ? key.hashCode() : 0;
			result = 31 * result + (int) (ts ^ (ts >>> 32));
			result = 31 * result + (source != null ? source.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return this.source + ":" + this.ts;
		}

		public static TypeSerializer<TestElem> serializer() {
			return TypeInformation.of(new TypeHint<TestElem>() {
			}).createSerializer(new ExecutionConfig());
		}
	}

	private static StreamRecord<TestElem> createStreamRecord(long ts, String source) {
		TestElem testElem = new TestElem(ts, source);
		return new StreamRecord<>(testElem, ts);
	}

	private void processElementsAndWatermarks(TestHarness testHarness) throws Exception {
		if (lhsFasterThanRhs) {
			// add to lhs
			for (int i = 1; i <= 4; i++) {
				testHarness.processElement1(createStreamRecord(i, "lhs"));
				testHarness.processWatermark1(new Watermark(i));
			}

			// add to rhs
			for (int i = 1; i <= 4; i++) {
				testHarness.processElement2(createStreamRecord(i, "rhs"));
				testHarness.processWatermark2(new Watermark(i));
			}
		} else {
			// add to rhs
			for (int i = 1; i <= 4; i++) {
				testHarness.processElement2(createStreamRecord(i, "rhs"));
				testHarness.processWatermark2(new Watermark(i));
			}

			// add to lhs
			for (int i = 1; i <= 4; i++) {
				testHarness.processElement1(createStreamRecord(i, "lhs"));
				testHarness.processWatermark1(new Watermark(i));
			}
		}
	}

	/**
	 * Custom test harness to avoid endless generics in all of the test code.
	 */
	private static class TestHarness extends KeyedTwoInputStreamOperatorTestHarness<String, TestElem, TestElem, Tuple2<TestElem, TestElem>> {

		TestHarness(
			TwoInputStreamOperator<TestElem, TestElem, Tuple2<TestElem, TestElem>> operator,
			KeySelector<TestElem, String> keySelector1,
			KeySelector<TestElem, String> keySelector2,
			TypeInformation<String> keyType) throws Exception {
			super(operator, keySelector1, keySelector2, keyType);
		}
	}
}
