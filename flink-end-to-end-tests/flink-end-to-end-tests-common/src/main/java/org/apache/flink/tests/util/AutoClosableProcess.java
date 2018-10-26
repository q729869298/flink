/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.tests.util;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Utility class to terminate a given {@link Process} when exiting a try-with-resources statement.
 */
public class AutoClosableProcess implements AutoCloseable {
	private final Process process;

	public AutoClosableProcess(Process process) {
		this.process = process;
	}

	public static AutoClosableProcess runNonBlocking(String step, String... commands) throws IOException {
		System.out.println("Starting " + step + ".");
		Process process = new ProcessBuilder()
			.command(commands)
			.inheritIO()
			.start();
		return new AutoClosableProcess(process);
	}

	public static void runBlocking(String step, String... commands) throws IOException {
		runBlocking(step, Duration.ofSeconds(30), commands);
	}

	public static void runBlocking(String step, Duration timeout, String... commands) throws IOException {
		System.out.println("Step started: " + step);
		Process process = new ProcessBuilder()
			.command(commands)
			.inheritIO()
			.start();

		try (AutoClosableProcess autoProcess = new AutoClosableProcess(process)) {
			final boolean b = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
			if (!b) {
				throw new TimeoutException();
			}
		} catch (TimeoutException | InterruptedException e) {
			throw new RuntimeException(step + " failed due to timeout.");
		}
		System.out.println("Step complete: " + step);
	}

	@Override
	public void close() throws IOException {
		if (process.isAlive()) {
			process.destroy();
			try {
				process.waitFor(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	public Process getProcess() {
		return process;
	}
}
