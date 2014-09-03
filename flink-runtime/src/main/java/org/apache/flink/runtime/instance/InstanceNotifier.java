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


package org.apache.flink.runtime.instance;

import java.util.List;

import org.apache.flink.runtime.instance.AllocatedResource;
import org.apache.flink.runtime.instance.InstanceListener;
import org.apache.flink.runtime.jobgraph.JobID;

/**
 * This class is an auxiliary class to send the notification
 * about the availability of an {@link org.apache.flink.runtime.instance.Instance} to the given {@link
 * InstanceListener} object. The notification must be sent from
 * a separate thread, otherwise the atomic operation of requesting an instance
 * for a vertex and switching to the state ASSIGNING could not be guaranteed.
 * This class is thread-safe.
 * 
 */
public class InstanceNotifier extends Thread {

	/**
	 * The {@link InstanceListener} object to send the notification to.
	 */
	private final InstanceListener instanceListener;

	/**
	 * The ID of the job the notification refers to.
	 */
	private final JobID jobID;

	/**
	 * The allocated resources the notification refers to.
	 */
	private final List<AllocatedResource> allocatedResources;

	/**
	 * Constructs a new instance notifier object.
	 * 
	 * @param instanceListener
	 *        the listener to send the notification to
	 * @param jobID
	 *        the ID of the job the notification refers to
	 * @param allocatedResources
	 *        the resources with has been allocated for the job
	 */
	public InstanceNotifier(final InstanceListener instanceListener, final JobID jobID,
							final List<AllocatedResource> allocatedResources) {
		this.instanceListener = instanceListener;
		this.jobID = jobID;
		this.allocatedResources = allocatedResources;
	}


	@Override
	public void run() {

		this.instanceListener.resourcesAllocated(this.jobID, this.allocatedResources);
	}
}
