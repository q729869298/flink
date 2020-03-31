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

package org.apache.flink.tests.util.flink;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.tests.util.AutoClosableProcess;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A wrapper around a Flink distribution.
 */
final class FlinkDistribution {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkDistribution.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private final Path opt;
	private final Path lib;
	private final Path conf;
	private final Path log;
	private final Path bin;

	private final Configuration defaultConfig;

	FlinkDistribution(Path distributionDir) {
		bin = distributionDir.resolve("bin");
		opt = distributionDir.resolve("opt");
		lib = distributionDir.resolve("lib");
		conf = distributionDir.resolve("conf");
		log = distributionDir.resolve("log");

		defaultConfig = new UnmodifiableConfiguration(
			GlobalConfiguration.loadConfiguration(conf.toAbsolutePath().toString()));
	}

	public void startJobManager() throws IOException {
		LOG.info("Starting Flink JobManager.");
		AutoClosableProcess.runBlocking(bin.resolve("jobmanager.sh").toAbsolutePath().toString(), "start");
	}

	public void startTaskManager() throws IOException {
		LOG.info("Starting Flink TaskManager.");
		AutoClosableProcess.runBlocking(bin.resolve("taskmanager.sh").toAbsolutePath().toString(), "start");
	}

	public void startFlinkCluster() throws IOException {
		LOG.info("Starting Flink cluster.");
		AutoClosableProcess.runBlocking(bin.resolve("start-cluster.sh").toAbsolutePath().toString());

		final OkHttpClient client = new OkHttpClient();

		final Request request = new Request.Builder()
			                        .get()
			                        .url("http://localhost:8081/taskmanagers")
			                        .build();

		Exception reportedException = null;
		for (int retryAttempt = 0; retryAttempt < 30; retryAttempt++) {
			try (Response response = client.newCall(request).execute()) {
				if (response.isSuccessful()) {
					final String json = response.body().string();
					final JsonNode taskManagerList = OBJECT_MAPPER.readTree(json)
						                                 .get("taskmanagers");

					if (taskManagerList != null && taskManagerList.size() > 0) {
						LOG.info("Dispatcher REST endpoint is up.");
						return;
					}
				}
			} catch (IOException ioe) {
				reportedException = ExceptionUtils.firstOrSuppressed(ioe, reportedException);
			}

			LOG.info("Waiting for dispatcher REST endpoint to come up...");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				reportedException = ExceptionUtils.firstOrSuppressed(e, reportedException);
			}
		}
		throw new AssertionError("Dispatcher REST endpoint did not start in time.", reportedException);
	}

	public Boolean checkFlinkCluster(int numTaskManagers) throws Exception {
		try (final RestClient restClient = new RestClient(
			RestClientConfiguration.fromConfiguration(new Configuration()), Executors.directExecutor())) {
			for (int retryAttempt = 0; retryAttempt < 30; retryAttempt++) {
				final CompletableFuture<TaskManagersInfo> localhost = restClient.sendRequest(
					// TODO: either ensure that at least one JM runs on the local machine, or use one of the
					//  configured JM hosts
					"localhost",
					8081,
					TaskManagersHeaders.getInstance(),
					EmptyMessageParameters.getInstance(),
					EmptyRequestBody.getInstance());

				try {
					final TaskManagersInfo taskManagersInfo = localhost.get(1, TimeUnit.SECONDS);
					final int numRunningTaskManagers = taskManagersInfo.getTaskManagerInfos().size();
					if (numRunningTaskManagers == numTaskManagers) {
						return true;
					} else {
						LOG.info("Waiting for task managers to come up. {}/{} are currently running.",
							numRunningTaskManagers, numTaskManagers);
					}
				} catch (InterruptedException e) {
					LOG.info("Waiting for dispatcher REST endpoint to come up...");
					Thread.currentThread().interrupt();
				} catch (TimeoutException | ExecutionException e) {
					// ExecutionExceptions may occur if leader election is still going on
					LOG.info("Waiting for dispatcher REST endpoint to come up...");
				}

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		} catch (ConfigurationException e) {
			throw new RuntimeException("Could not create RestClient.", e);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return false;
	}

	public void stopFlinkCluster() throws IOException {
		LOG.info("Stopping Flink cluster.");
		AutoClosableProcess.runBlocking(bin.resolve("stop-cluster.sh").toAbsolutePath().toString());
	}

	public JobID submitJob(final JobSubmission jobSubmission) throws IOException {
		final List<String> commands = new ArrayList<>(4);
		commands.add(bin.resolve("flink").toString());
		commands.add("run");
		if (jobSubmission.isDetached()) {
			commands.add("-d");
		}
		if (jobSubmission.getParallelism() > 0) {
			commands.add("-p");
			commands.add(String.valueOf(jobSubmission.getParallelism()));
		}
		commands.add(jobSubmission.getJar().toAbsolutePath().toString());
		commands.addAll(jobSubmission.getArguments());

		LOG.info("Running {}.", commands.stream().collect(Collectors.joining(" ")));

		final Pattern pattern = jobSubmission.isDetached()
			                        ? Pattern.compile("Job has been submitted with JobID (.*)")
			                        : Pattern.compile("Job with JobID (.*) has finished.");

		final CompletableFuture<String> rawJobIdFuture = new CompletableFuture<>();
		final Consumer<String> stdoutProcessor = string -> {
			LOG.info(string);
			Matcher matcher = pattern.matcher(string);
			if (matcher.matches()) {
				rawJobIdFuture.complete(matcher.group(1));
			}
		};

		try (AutoClosableProcess flink =
			     AutoClosableProcess.create(commands.toArray(new String[0])).setStdoutProcessor(
				     stdoutProcessor).runNonBlocking()) {
			if (jobSubmission.isDetached()) {
				try {
					flink.getProcess().waitFor();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}

			try {
				return JobID.fromHexString(rawJobIdFuture.get(1, TimeUnit.MINUTES));
			} catch (Exception e) {
				throw new IOException("Could not determine Job ID.", e);
			}
		}
	}

	public void submitSQLJob(SQLJobSubmission job) throws IOException {
		final List<String> commands = new ArrayList<>();
		commands.add(bin.resolve("sql-client.sh").toAbsolutePath().toString());
		commands.add("embedded");
		job.getDefaultEnvFile().ifPresent(defaultEnvFile -> {
			commands.add("--defaults");
			commands.add(defaultEnvFile);
		});
		job.getSessionEnvFile().ifPresent(sessionEnvFile -> {
			commands.add("--environment");
			commands.add(sessionEnvFile);
		});
		for (String jar : job.getJars()) {
			commands.add("--jar");
			commands.add(jar);
		}
		commands.add("--update");
		commands.add("\"" + job.getSQL() + "\"");

		AutoClosableProcess.runBlocking(commands.toArray(new String[0]));
	}

	public void moveJar(JarMove move) throws IOException {
		final Path source = mapJarLocationToPath(move.getSource());
		final Path target = mapJarLocationToPath(move.getTarget());

		final Optional<Path> jarOptional;
		try (Stream<Path> files = Files.walk(source)) {
			jarOptional = files
				              .filter(path -> path.getFileName().toString().startsWith(move.getJarNamePrefix()))
				              .findFirst();
		}
		if (jarOptional.isPresent()) {
			final Path sourceJar = jarOptional.get();
			final Path targetJar = target.resolve(sourceJar.getFileName());
			Files.copy(sourceJar, targetJar);
		} else {
			throw new FileNotFoundException(
				"No jar could be found matching the pattern " + move.getJarNamePrefix() + ".");
		}
	}

	private Path mapJarLocationToPath(JarLocation location) {
		switch (location) {
			case LIB:
				return lib;
			case OPT:
				return opt;
			default:
				throw new IllegalStateException();
		}
	}

	public void appendConfiguration(Configuration config) throws IOException {
		final Configuration mergedConfig = new Configuration();
		mergedConfig.addAll(defaultConfig);
		mergedConfig.addAll(config);

		final List<String> configurationLines = mergedConfig.toMap().entrySet().stream()
			                                        .map(entry -> entry.getKey() + ": " + entry.getValue())
			                                        .collect(Collectors.toList());

		Files.write(conf.resolve("flink-conf.yaml"), configurationLines);
	}

	public void setJobMasterHosts(Collection<String> jobMasterHosts) throws IOException {
		Files.write(conf.resolve("masters"), jobMasterHosts);
	}

	public void setTaskExecutorHosts(Collection<String> taskExecutorHosts) throws IOException {
		Files.write(conf.resolve("slaves"), taskExecutorHosts);
	}

	public Stream<String> searchAllLogs(Pattern pattern, Function<Matcher, String> matchProcessor) throws IOException {
		final List<String> matches = new ArrayList<>(2);

		try (Stream<Path> logFilesStream = Files.list(log)) {
			final Iterator<Path> logFiles = logFilesStream.iterator();
			while (logFiles.hasNext()) {
				final Path logFile = logFiles.next();
				if (!logFile.getFileName().toString().endsWith(".log")) {
					// ignore logs for previous runs that have a number suffix
					continue;
				}
				try (BufferedReader br = new BufferedReader(
					new InputStreamReader(new FileInputStream(logFile.toFile()), StandardCharsets.UTF_8))) {
					String line;
					while ((line = br.readLine()) != null) {
						Matcher matcher = pattern.matcher(line);
						if (matcher.matches()) {
							matches.add(matchProcessor.apply(matcher));
						}
					}
				}
			}
		}
		return matches.stream();
	}

	public void copyLogsTo(Path targetDirectory) throws IOException {
		Files.createDirectories(targetDirectory);
		TestUtils.copyDirectory(log, targetDirectory);
	}
}
