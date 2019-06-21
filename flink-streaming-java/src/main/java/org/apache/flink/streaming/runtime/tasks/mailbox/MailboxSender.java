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

package org.apache.flink.streaming.runtime.tasks.mailbox;

import javax.annotation.Nonnull;

/**
 * Producer-facing side of the {@link Mailbox} interface. This is used to enqueue letters. Multiple producers threads
 * can put to the same mailbox.
 */
public interface MailboxSender {

	/**
	 * Enqueues the given letter to the mailbox, if capacity is available. On success, this returns <code>true</code>
	 * and <code>false</code> if the mailbox was already full.
	 *
	 * @param letter the letter to enqueue.
	 * @return <code>true</code> iff successful.
	 * @throws MailboxStateException if the mailbox is quiesced or closed.
	 */
	boolean tryPutMail(@Nonnull Runnable letter) throws MailboxStateException;

	/**
	 * Enqueues the given letter to the mailbox and blocks until there is capacity for a successful put.
	 *
	 * @param letter the letter to enqueue.
	 * @throws InterruptedException on interruption.
	 * @throws MailboxStateException if the mailbox is quiesced or closed.
	 */
	void putMail(@Nonnull Runnable letter) throws InterruptedException,  MailboxStateException;
}
