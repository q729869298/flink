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

package org.apache.flink.runtime.rpc.akka;

import akka.util.Timeout;

import org.apache.flink.runtime.akka.AkkaUtils;

import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcService;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class MainThreadValidationTest {

	@Test
	public void failIfNotInMainThread() {
		// test if assertions are activated. The test only works if assertions are loaded.
		try {
			assert false;
			// apparently they are not activated
			return;
		} catch (AssertionError ignored) {}

		// actual test
		AkkaRpcService akkaRpcService = new AkkaRpcService(
				AkkaUtils.createDefaultActorSystem(),
				new Timeout(10000, TimeUnit.MILLISECONDS));

		try {
			TestEndpoint testEndpoint = new TestEndpoint(akkaRpcService);

			// this works, because it is executed as an RPC call
			testEndpoint.getSelf().someConcurrencyCriticalFunction();

			// this fails, because it is executed directly
			boolean exceptionThrown;
			try {
				testEndpoint.someConcurrencyCriticalFunction();
				exceptionThrown = false;
			}
			catch (AssertionError e) {
				exceptionThrown = true;
			}
			assertTrue("should fail with an assertion error", exceptionThrown);

			akkaRpcService.stopServer(testEndpoint.getSelf());
		}
		finally {
			akkaRpcService.stopService();
		}
	}

	// ------------------------------------------------------------------------
	//  test RPC endpoint
	// ------------------------------------------------------------------------

	interface TestGateway extends RpcGateway {

		void someConcurrencyCriticalFunction();
	}

	@SuppressWarnings("unused")
	public static class TestEndpoint extends RpcEndpoint<TestGateway> {

		public TestEndpoint(RpcService rpcService) {
			super(rpcService);
		}

		@RpcMethod
		public void someConcurrencyCriticalFunction() {
			validateRunsInMainThread();
		}
	}
}
