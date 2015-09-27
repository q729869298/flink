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

package org.apache.flink.runtime.webmonitor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.curator.test.TestingServer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.StreamingMode;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.leaderelection.TestingListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.webmonitor.files.MimeTypes;
import org.apache.flink.runtime.webmonitor.testutils.HttpTestClient;
import org.junit.Test;
import org.powermock.reflect.Whitebox;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WebRuntimeMonitorITCase {

	private final static FiniteDuration TestTimeout = new FiniteDuration(2, TimeUnit.MINUTES);

	private final String MAIN_RESOURCES_PATH = getClass().getResource("/../classes/web").getPath();

	/**
	 * Tests operation of the monitor in standalone operation.
	 */
	@Test
	public void testStandaloneWebRuntimeMonitor() throws Exception {
		final Deadline deadline = TestTimeout.fromNow();

		TestingCluster flink = null;
		WebRuntimeMonitor webMonitor = null;

		try {
			// Flink w/o a web monitor
			flink = new TestingCluster(new Configuration());
			flink.start(true);

			ActorSystem jmActorSystem = flink.jobManagerActorSystems().get().head();
			ActorRef jmActor = flink.jobManagerActors().get().head();

			Configuration monitorConfig = new Configuration();
			monitorConfig.setString(WebMonitorConfig.JOB_MANAGER_WEB_DOC_ROOT_KEY, MAIN_RESOURCES_PATH);
			monitorConfig.setBoolean(ConfigConstants.JOB_MANAGER_NEW_WEB_FRONTEND_KEY, true);

			// Needs to match the leader address from the leader retrieval service
			String jobManagerAddress = AkkaUtils.getAkkaURL(jmActorSystem, jmActor);

			webMonitor = new WebRuntimeMonitor(monitorConfig, flink.createLeaderRetrievalService(),
					jmActorSystem);

			webMonitor.start(jobManagerAddress);

			try (HttpTestClient client = new HttpTestClient("localhost", webMonitor.getServerPort())) {
				String expected = new Scanner(new File(MAIN_RESOURCES_PATH + "/index.html"))
						.useDelimiter("\\A").next();

				// Request the file from the web server
				client.sendGetRequest("index.html", deadline.timeLeft());

				HttpTestClient.SimpleHttpResponse response = client.getNextResponse(deadline.timeLeft());

				assertEquals(HttpResponseStatus.OK, response.getStatus());
				assertEquals(response.getType(), MimeTypes.getMimeTypeForExtension("html"));
				assertEquals(expected, response.getContent());

				// Simple overview request
				client.sendGetRequest("/overview", deadline.timeLeft());

				response = client.getNextResponse(deadline.timeLeft());
				assertEquals(HttpResponseStatus.OK, response.getStatus());
				assertEquals(response.getType(), MimeTypes.getMimeTypeForExtension("json"));
				assertTrue(response.getContent().contains("\"taskmanagers\":1"));
			}
		}
		finally {
			if (flink != null) {
				flink.shutdown();
			}

			if (webMonitor != null) {
				webMonitor.stop();
			}
		}
	}

	/**
	 * Tests that the monitor associated with the following job manager redirects to the leader.
	 */
	@Test
	public void testRedirectToLeader() throws Exception {
		final Deadline deadline = TestTimeout.fromNow();

		ActorSystem[] jobManagerSystem = new ActorSystem[2];
		WebRuntimeMonitor[] webMonitor = new WebRuntimeMonitor[2];

		try (TestingServer zooKeeper = new TestingServer()) {
			final Configuration config = new Configuration();
			config.setString(WebMonitorConfig.JOB_MANAGER_WEB_DOC_ROOT_KEY, MAIN_RESOURCES_PATH);
			config.setBoolean(ConfigConstants.JOB_MANAGER_NEW_WEB_FRONTEND_KEY, true);
			config.setInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, 0);
			config.setString(ConfigConstants.RECOVERY_MODE, "ZOOKEEPER");
			config.setString(ConfigConstants.ZOOKEEPER_QUORUM_KEY, zooKeeper.getConnectString());


			for (int i = 0; i < jobManagerSystem.length; i++) {
				jobManagerSystem[i] = AkkaUtils.createActorSystem(new Configuration(),
						new Some<>(new Tuple2<String, Object>("localhost", 0)));
			}

			for (int i = 0; i < webMonitor.length; i++) {
				LeaderRetrievalService lrs = ZooKeeperUtils.createLeaderRetrievalService(config);
				webMonitor[i] = new WebRuntimeMonitor(config, lrs, jobManagerSystem[i]);
			}

			ActorRef[] jobManager = new ActorRef[2];
			String[] jobManagerAddress = new String[2];
			for (int i = 0; i < jobManager.length; i++) {
				Configuration jmConfig = config.clone();
				jmConfig.setInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY,
						webMonitor[i].getServerPort());

				jobManager[i] = JobManager.startJobManagerActors(
						jmConfig, jobManagerSystem[i], StreamingMode.STREAMING)._1();

				jobManagerAddress[i] = AkkaUtils.getAkkaURL(jobManagerSystem[i], jobManager[i]);
				webMonitor[i].start(jobManagerAddress[i]);
			}

			LeaderRetrievalService lrs = ZooKeeperUtils.createLeaderRetrievalService(config);
			TestingListener leaderListener = new TestingListener();
			lrs.start(leaderListener);

			leaderListener.waitForNewLeader(deadline.timeLeft().toMillis());

			String leaderAddress = leaderListener.getAddress();

			int leaderIndex = leaderAddress.equals(jobManagerAddress[0]) ? 0 : 1;

			ActorSystem leadingSystem = jobManagerSystem[leaderIndex];
			WebMonitor leadingWebMonitor = webMonitor[leaderIndex];
			WebMonitor followingWebMonitor = webMonitor[(leaderIndex + 1) % 2];

			// For test stability reason we have to wait until we are sure that both leader
			// listeners have been notified.
			JobManagerArchiveRetriever followingRetriever = Whitebox
					.getInternalState(followingWebMonitor, "retriever");

			while (deadline.hasTimeLeft() && followingRetriever.getRedirectAddress() == null) {
				Thread.sleep(100);
			}

			try (
					HttpTestClient leaderClient = new HttpTestClient(
							"localhost", leadingWebMonitor.getServerPort());

					HttpTestClient followingClient = new HttpTestClient(
							"localhost", followingWebMonitor.getServerPort())) {

				String expected = new Scanner(new File(MAIN_RESOURCES_PATH + "/index.html"))
						.useDelimiter("\\A").next();

				// Request the file from the leaading web server
				leaderClient.sendGetRequest("index.html", deadline.timeLeft());

				HttpTestClient.SimpleHttpResponse response = leaderClient.getNextResponse(deadline.timeLeft());
				assertEquals(HttpResponseStatus.OK, response.getStatus());
				assertEquals(response.getType(), MimeTypes.getMimeTypeForExtension("html"));
				assertEquals(expected, response.getContent());

				// Request the file from the following web server
				followingClient.sendGetRequest("index.html", deadline.timeLeft());
				response = followingClient.getNextResponse(deadline.timeLeft());
				assertEquals(HttpResponseStatus.TEMPORARY_REDIRECT, response.getStatus());
				assertTrue(response.getLocation().contains("" + leadingWebMonitor.getServerPort()));

				// Kill the leader
				leadingSystem.shutdown();

				// Wait for the notification of the follower
				while (deadline.hasTimeLeft() && followingRetriever.getRedirectAddress() != null) {
					Thread.sleep(100);
				}

				// Same request to the new leader
				followingClient.sendGetRequest("index.html", deadline.timeLeft());

				response = followingClient.getNextResponse(deadline.timeLeft());
				assertEquals(HttpResponseStatus.OK, response.getStatus());
				assertEquals(response.getType(), MimeTypes.getMimeTypeForExtension("html"));
				assertEquals(expected, response.getContent());

				// Simple overview request
				followingClient.sendGetRequest("/overview", deadline.timeLeft());

				response = followingClient.getNextResponse(deadline.timeLeft());
				assertEquals(HttpResponseStatus.OK, response.getStatus());
				assertEquals(response.getType(), MimeTypes.getMimeTypeForExtension("json"));
				assertTrue(response.getContent().contains("\"taskmanagers\":1") ||
						response.getContent().contains("\"taskmanagers\":0"));
			}
		}
		finally {
			for (ActorSystem system : jobManagerSystem) {
				if (system != null) {
					system.shutdown();
				}
			}

			for (WebMonitor monitor : webMonitor) {
				monitor.stop();
			}
		}
	}
}
