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

package org.apache.flink.streaming.api.functions.source;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketTextStreamFunction extends RichSourceFunction<String> {

	protected static final Logger LOG = LoggerFactory.getLogger(SocketTextStreamFunction.class);

	private static final long serialVersionUID = 1L;

	private String hostname;
	private int port;
	private String delimiter;
	private long maxRetry;
	private boolean retryForever;
	private Socket socket;
	private static final int CONNECTION_TIMEOUT_TIME = 0;
	private static final int CONNECTION_RETRY_SLEEP = 1000;

	private volatile boolean isRunning;

	public SocketTextStreamFunction(String hostname, int port, String delimiter, long maxRetry) {
		this.hostname = hostname;
		this.port = port;
		this.delimiter = delimiter;
		this.maxRetry = maxRetry;
		this.retryForever = maxRetry < 0;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		socket = new Socket();
		socket.connect(new InetSocketAddress(hostname, port), CONNECTION_TIMEOUT_TIME);
		isRunning = true;
	}

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		streamFromSocket(ctx, socket);
	}

	public void streamFromSocket(SourceContext<String> ctx, Socket socket) throws Exception {

		try {
			StringBuffer buffer = new StringBuffer();
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					socket.getInputStream()));

			while (isRunning) {
				int charsRead;
				// int data;
				char [] cbuff = new char[ delimiter.length() ];
				try {
					//data = reader.read();
					charsRead = reader.read(cbuff);
				} catch (SocketException e) {
					if (!isRunning) {
						break;
					} else {
						throw e;
					}
				}

				if (charsRead == -1) {
					socket.close();
					long retry = 0;
					boolean success = false;
					while (retry < maxRetry && !success) {
						if (!retryForever) {
							retry++;
						}
						LOG.warn("Lost connection to server socket. Retrying in "
								+ (CONNECTION_RETRY_SLEEP / 1000) + " seconds...");
						try {
							socket = new Socket();
							socket.connect(new InetSocketAddress(hostname, port), CONNECTION_TIMEOUT_TIME);
							success = true;
						} catch (ConnectException ce) {
							Thread.sleep(CONNECTION_RETRY_SLEEP);
						}
					}

					if (success) {
						LOG.info("Server socket is reconnected.");
					} else {
						LOG.error("Could not reconnect to server socket.");
						break;
					}
					reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					continue;
				}

				//if (String.valueOf((char)data).equals(delimiter)) {
				if (buffer.indexOf(delimiter) != -1) {
					ctx.collect(buffer.substring(0, buffer.indexOf(delimiter)));
					buffer = new StringBuffer(buffer.substring(buffer.indexOf(delimiter) + delimiter.length()));

				//} else if (data != '\r') { // ignore carriage return
				} else {
					while (ArrayUtils.contains(cbuff, '\r'))
						ArrayUtils.removeElement(cbuff, 'r');

					buffer.append(cbuff);
				}
			}

			if (buffer.length() > 0) {
				ctx.collect(buffer.toString());
			}
		} finally {
			socket.close();
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
		if (socket != null && !socket.isClosed()) {
			try {
				socket.close();
			} catch (IOException e) {
				if (LOG.isErrorEnabled()) {
					LOG.error("Could not close open socket");
				}
			}
		}
	}
}
