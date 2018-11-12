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

package org.apache.flink.streaming.connectors.pubsub;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.junit.Before;
import org.junit.Test;
import scala.sys.process.ProcessBuilder;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * Test for {@link Bound}.
 */
public class BoundTest {
	private static class CountingSourceFunction implements SourceFunction<Object> {
		int countRun = 0;
		@Override
		public void run(SourceContext sourceContext) {
			countRun++;
		}

		int countCancel = 0;
		@Override
		public void cancel() {
			countCancel++;
		}

		public void verify(int expectedRun, int expectedCancel) {
			assertEquals("Wrong number of calls to run()", expectedRun, countRun);
			assertEquals("Wrong number of calls to cancel()", expectedCancel, countCancel);
		}
	}

	private CountingSourceFunction sourceFunction;
	@Before
	public void initSourceFunction() {
		sourceFunction = new CountingSourceFunction();
	}

	@Test
	public void testNoShutdownBeforeCounterLimit() {
		Bound<Object> bound = Bound.boundByAmountOfMessages(10);
		bound.start(sourceFunction);
		sleep(150L);

		bound.receivedMessage();
		sourceFunction.verify(0, 0);
	}

	@Test
	public void testShutdownOnCounterLimit() {
		Bound<Object> bound = Bound.boundByAmountOfMessages(3);
		bound.start(sourceFunction);

		bound.receivedMessage();
		bound.receivedMessage();
		bound.receivedMessage();

		sourceFunction.verify(0, 1);
	}

	@Test
	public void testNoShutdownBeforeTimerLimit() {
		Bound<Object> bound = Bound.boundByTimeSinceLastMessage(1000L);
		bound.start(sourceFunction);
		for (int i = 0; i < 10; i++) {
			bound.receivedMessage();
		}

		sourceFunction.verify(0, 0);
	}

	@Test
	public void testShutdownAfterTimerLimitNoMessageReceived() {
		Bound<Object> bound = Bound.boundByTimeSinceLastMessage(100L);
		bound.start(sourceFunction);
		sleep(250L);
		sourceFunction.verify(0, 1);
	}

	@Test
	public void testShutdownAfterTimerLimitAfterMessageReceived() {
		Bound<Object> bound = Bound.boundByTimeSinceLastMessage(100L);
		bound.start(sourceFunction);
		sleep(50L);

		bound.receivedMessage();
		sleep(50L);
		sourceFunction.verify(0, 0);

		sleep(250L);
		sourceFunction.verify(0, 1);
	}

	@Test
	public void testCounterOrTimerMaxMessages() {
		Bound<Object> bound = Bound.boundByAmountOfMessagesOrTimeSinceLastMessage(3, 1000L);
		bound.start(sourceFunction);

		bound.receivedMessage();
		bound.receivedMessage();
		bound.receivedMessage();

		sourceFunction.verify(0, 1);
	}

	@Test
	public void testCounterOrTimerTimerElapsed() {
		Bound<Object> bound = Bound.boundByAmountOfMessagesOrTimeSinceLastMessage(1L, 100L);
		bound.start(sourceFunction);
		sleep(250L);
		sourceFunction.verify(0, 1);
	}

	@Test(expected = IllegalStateException.class)
	public void testExceptionThrownIfStartNotCalled() {
		Bound<Object> bound = Bound.boundByAmountOfMessagesOrTimeSinceLastMessage(1L, 100L);
		bound.receivedMessage();
	}

	@Test(expected = IllegalStateException.class)
	public void testExceptionThrownIfStartCalledTwice() {
		Bound<Object> bound = Bound.boundByAmountOfMessagesOrTimeSinceLastMessage(1L, 100L);
		bound.start(sourceFunction);
		bound.start(sourceFunction);
	}

	private void sleep(long sleepTime) {
		try {
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {
			// Ignore any exceptions
		}
	}
}
