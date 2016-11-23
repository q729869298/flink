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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.security.MessageDigest;

import static org.apache.flink.runtime.blob.BlobServerProtocol.BUFFER_SIZE;
import static org.apache.flink.runtime.blob.BlobServerProtocol.CONTENT_ADDRESSABLE;
import static org.apache.flink.runtime.blob.BlobServerProtocol.DELETE_OPERATION;
import static org.apache.flink.runtime.blob.BlobServerProtocol.GET_OPERATION;
import static org.apache.flink.runtime.blob.BlobServerProtocol.JOB_ID_SCOPE;
import static org.apache.flink.runtime.blob.BlobServerProtocol.MAX_KEY_LENGTH;
import static org.apache.flink.runtime.blob.BlobServerProtocol.NAME_ADDRESSABLE;
import static org.apache.flink.runtime.blob.BlobServerProtocol.PUT_OPERATION;
import static org.apache.flink.runtime.blob.BlobServerProtocol.RETURN_ERROR;
import static org.apache.flink.runtime.blob.BlobServerProtocol.RETURN_OKAY;
import static org.apache.flink.runtime.blob.BlobUtils.closeSilently;
import static org.apache.flink.runtime.blob.BlobUtils.readFully;
import static org.apache.flink.runtime.blob.BlobUtils.readLength;
import static org.apache.flink.runtime.blob.BlobUtils.writeLength;

/**
 * A BLOB connection handles a series of requests from a particular BLOB client.
 */
class BlobServerConnection extends Thread {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(BlobServerConnection.class);

	/** The socket to communicate with the client. */
	private final Socket clientSocket;

	/** The BLOB server. */
	private final BlobServer blobServer;

	/** The blob store. */
	private final BlobStore blobStore;

	/**
	 * Creates a new BLOB connection for a client request
	 * 
	 * @param clientSocket The socket to read/write data.
	 * @param blobServer The BLOB server.
	 */
	BlobServerConnection(Socket clientSocket, BlobServer blobServer) {
		super("BLOB connection for " + clientSocket.getRemoteSocketAddress().toString());
		setDaemon(true);

		if (blobServer == null) {
			throw new NullPointerException();
		}

		this.clientSocket = clientSocket;
		this.blobServer = blobServer;
		this.blobStore = blobServer.getBlobStore();
	}

	// --------------------------------------------------------------------------------------------
	//  Connection / Thread methods
	// --------------------------------------------------------------------------------------------

	/**
	 * Main connection work method. Accepts requests until the other side closes the connection.
	 */
	@Override
	public void run() {
		try {
			final InputStream inputStream = this.clientSocket.getInputStream();
			final OutputStream outputStream = this.clientSocket.getOutputStream();
			final byte[] buffer = new byte[BUFFER_SIZE];

			while (true) {
				// Read the requested operation
				final int operation = inputStream.read();
				if (operation < 0) {
					// done, no one is asking anything from us
					return;
				}

				switch (operation) {
				case PUT_OPERATION:
					put(inputStream, outputStream, buffer);
					break;
				case GET_OPERATION:
					get(inputStream, outputStream, buffer);
					break;
				case DELETE_OPERATION:
					delete(inputStream, outputStream, buffer);
					break;
				default:
					throw new IOException("Unknown operation " + operation);
				}
			}
		}
		catch (SocketException e) {
			// this happens when the remote site closes the connection
			LOG.debug("Socket connection closed", e);
		}
		catch (Throwable t) {
			LOG.error("Error while executing BLOB connection.", t);
		}
		finally {
			try {
				if (clientSocket != null) {
					clientSocket.close();
				}
			} catch (Throwable t) {
				LOG.debug("Exception while closing BLOB server connection socket.", t);
			}

			blobServer.unregisterConnection(this);
		}
	}

	/**
	 * Closes the connection socket and lets the thread exit.
	 */
	public void close() {
		closeSilently(clientSocket, LOG);
		interrupt();
	}

	// --------------------------------------------------------------------------------------------
	//  Actions
	// --------------------------------------------------------------------------------------------

	/**
	 * Handles an incoming GET request from a BLOB client.
	 * 
	 * @param inputStream
	 *        the input stream to read incoming data from
	 * @param outputStream
	 *        the output stream to send data back to the client
	 * @param buf
	 *        an auxiliary buffer for data serialization/deserialization
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading/writing data from/to the respective streams
	 */
	private void get(InputStream inputStream, OutputStream outputStream, byte[] buf) throws IOException {

		FileStatus blobFile;
		try {
			final int contentAddressable = inputStream.read();

			if (contentAddressable < 0) {
				throw new EOFException("Premature end of GET request");
			}
			if (contentAddressable == NAME_ADDRESSABLE) {
				// Receive the job ID and key
				byte[] jidBytes = new byte[JobID.SIZE];
				readFully(inputStream, jidBytes, 0, JobID.SIZE, "JobID");

				JobID jobID = JobID.fromByteArray(jidBytes);
				String key = readKey(buf, inputStream);
				// will throw a FileNotFoundException if the file does not exist
				blobFile = blobStore.getFileStatus(jobID, key);
			}
			else if (contentAddressable == CONTENT_ADDRESSABLE) {
				final BlobKey key = BlobKey.readFromInputStream(inputStream);
				// will throw a FileNotFoundException if the file does not exist
				blobFile = blobStore.getFileStatus(key);
			}
			else {
				throw new IOException("Unknown type of BLOB addressing.");
			}

			// we restrict the length field to an integer below
			if (blobFile.getLen() > Integer.MAX_VALUE) {
				throw new IOException("BLOB size exceeds the maximum size (2 GB).");
			}

			outputStream.write(RETURN_OKAY);

			// up to here, an error can give a good message
		}
		catch (Throwable t) {
			LOG.error("GET operation failed", t);
			try {
				writeErrorToStream(outputStream, t);
			}
			catch (IOException e) {
				// since we are in an exception case, it means not much that we could not send the error
				// ignore this
			}
			clientSocket.close();
			return;
		}

		// from here on, we started sending data, so all we can do is close the connection when something happens
		try {
			int blobLen = (int) blobFile.getLen();
			writeLength(blobLen, outputStream);

			try (FSDataInputStream fis = blobStore.open(blobFile)) {
				int bytesRemaining = blobLen;
				while (bytesRemaining > 0) {
					int read = fis.read(buf);
					if (read < 0) {
						throw new IOException("Premature end of BLOB file stream for " + blobFile.getPath());
					}
					outputStream.write(buf, 0, read);
					bytesRemaining -= read;
				}
			}
		}
		catch (SocketException e) {
			// happens when the other side disconnects
			LOG.debug("Socket connection closed", e);
		}
		catch (Throwable t) {
			LOG.error("GET operation failed", t);
			clientSocket.close();
		}
	}

	/**
	 * Handles an incoming PUT request from a BLOB client.
	 * 
	 * @param inputStream The input stream to read incoming data from.
	 * @param outputStream The output stream to send data back to the client.
	 * @param buf An auxiliary buffer for data serialization/deserialization.
	 */
	private void put(InputStream inputStream, OutputStream outputStream, byte[] buf) throws IOException {
		JobID jobID = null;
		String key = null;
		MessageDigest md = null;

		String incomingFile = null;
		FSDataOutputStream fos = null;

		try {
			final int contentAddressable = inputStream.read();
			if (contentAddressable < 0) {
				throw new EOFException("Premature end of PUT request");
			}

			if (contentAddressable == NAME_ADDRESSABLE) {
				// Receive the job ID and key
				byte[] jidBytes = new byte[JobID.SIZE];
				readFully(inputStream, jidBytes, 0, JobID.SIZE, "JobID");
				jobID = JobID.fromByteArray(jidBytes);
				key = readKey(buf, inputStream);
			}
			else if (contentAddressable == CONTENT_ADDRESSABLE) {
				md = BlobUtils.createMessageDigest();
			}
			else {
				throw new IOException("Unknown type of BLOB addressing.");
			}

			if (LOG.isDebugEnabled()) {
				if (contentAddressable == NAME_ADDRESSABLE) {
					LOG.debug(String.format("Received PUT request for BLOB under %s / \"%s\"", jobID, key));
				} else {
					LOG.debug("Received PUT request for content addressable BLOB");
				}
			}

			incomingFile = blobStore.getTempFilename();
			fos = blobStore.createTempFile(incomingFile);

			while (true) {
				final int bytesExpected = readLength(inputStream);
				if (bytesExpected == -1) {
					// done
					break;
				}
				if (bytesExpected > BUFFER_SIZE) {
					throw new IOException("Unexpected number of incoming bytes: " + bytesExpected);
				}

				readFully(inputStream, buf, 0, bytesExpected, "buffer");
				fos.write(buf, 0, bytesExpected);

				if (md != null) {
					md.update(buf, 0, bytesExpected);
				}
			}
			fos.close();
			fos = null;

			if (contentAddressable == NAME_ADDRESSABLE) {
				blobStore.persistTempFile(incomingFile, jobID, key);
				incomingFile = null;

				outputStream.write(RETURN_OKAY);
			}
			else {
				BlobKey blobKey = new BlobKey(md.digest());
				blobStore.persistTempFile(incomingFile, blobKey);
				incomingFile = null;

				// Return computed key to client for validation
				outputStream.write(RETURN_OKAY);
				blobKey.writeToOutputStream(outputStream);
			}
		}
		catch (SocketException e) {
			// happens when the other side disconnects
			LOG.debug("Socket connection closed", e);
		}
		catch (Throwable t) {
			LOG.error("PUT operation failed", t);
			try {
				writeErrorToStream(outputStream, t);
			}
			catch (IOException e) {
				// since we are in an exception case, it means not much that we could not send the error
				// ignore this
			}
			clientSocket.close();
		}
		finally {
			if (fos != null) {
				try {
					fos.close();
				} catch (Throwable t) {
					LOG.warn("Cannot close stream to BLOB staging file", t);
				}
			}
			if (incomingFile != null) {
				try {
					blobStore.deleteTempFile(incomingFile);
				} catch (Throwable t) {
					LOG.warn("Cannot delete BLOB server staging file " + incomingFile);
				}
			}
		}
	}

	/**
	 * Handles an incoming DELETE request from a BLOB client.
	 * 
	 * @param inputStream The input stream to read the request from.
	 * @param outputStream The output stream to write the response to.
	 * @throws java.io.IOException Thrown if an I/O error occurs while reading the request data from the input stream.
	 */
	private void delete(InputStream inputStream, OutputStream outputStream, byte[] buf) throws IOException {

		try {
			int type = inputStream.read();
			if (type < 0) {
				throw new EOFException("Premature end of DELETE request");
			}

			if (type == CONTENT_ADDRESSABLE) {
				BlobKey key = BlobKey.readFromInputStream(inputStream);
				if (!blobStore.delete(key)) {
					throw new IOException("Cannot delete BLOB file " + key.toString());
				}
			}
			else if (type == NAME_ADDRESSABLE) {
				byte[] jidBytes = new byte[JobID.SIZE];
				readFully(inputStream, jidBytes, 0, JobID.SIZE, "JobID");
				JobID jobID = JobID.fromByteArray(jidBytes);

				String key = readKey(buf, inputStream);
				if (!blobStore.delete(jobID, key)) {
					throw new IOException("Cannot delete BLOB file " + key.toString());
				}
			}
			else if (type == JOB_ID_SCOPE) {
				byte[] jidBytes = new byte[JobID.SIZE];
				readFully(inputStream, jidBytes, 0, JobID.SIZE, "JobID");
				JobID jobID = JobID.fromByteArray(jidBytes);

				blobStore.deleteAll(jobID);
			}
			else {
				throw new IOException("Unrecognized addressing type: " + type);
			}

			outputStream.write(RETURN_OKAY);
		}
		catch (Throwable t) {
			LOG.error("DELETE operation failed", t);
			try {
				writeErrorToStream(outputStream, t);
			}
			catch (IOException e) {
				// since we are in an exception case, it means not much that we could not send the error
				// ignore this
			}
			clientSocket.close();
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Reads the key of a BLOB from the given input stream.
	 * 
	 * @param buf
	 *        auxiliary buffer to data deserialization
	 * @param inputStream
	 *        the input stream to read the key from
	 * @return the key of a BLOB
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading the key data from the input stream
	 */
	private static String readKey(byte[] buf, InputStream inputStream) throws IOException {
		final int keyLength = readLength(inputStream);
		if (keyLength > MAX_KEY_LENGTH) {
			throw new IOException("Unexpected key length " + keyLength);
		}

		readFully(inputStream, buf, 0, keyLength, "BlobKey");
		return new String(buf, 0, keyLength, BlobUtils.DEFAULT_CHARSET);
	}

	/**
	 * Writes to the output stream the error return code, and the given exception in serialized form.
	 *
	 * @param out Thr output stream to write to.
	 * @param t The exception to send.
	 * @throws IOException Thrown, if the output stream could not be written to.
	 */
	private static void writeErrorToStream(OutputStream out, Throwable t) throws IOException {
		byte[] bytes = InstantiationUtil.serializeObject(t);
		out.write(RETURN_ERROR);
		writeLength(bytes.length, out);
		out.write(bytes);
	}
}
