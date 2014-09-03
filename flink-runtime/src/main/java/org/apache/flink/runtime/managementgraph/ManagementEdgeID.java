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


package org.apache.flink.runtime.managementgraph;

import org.apache.flink.runtime.AbstractID;
import org.apache.flink.runtime.io.network.channels.ChannelID;

/**
 * A management edge ID uniquely identifies a {@link ManagementEdge}.
 * <p>
 * This class is not thread-safe.
 * 
 */
public class ManagementEdgeID extends AbstractID {

	/**
	 * Initializes ManagementEdgeID.
	 */
	ManagementEdgeID() {
	}

	/**
	 * A ManagementEdgeID is derived from the #{@link ChannelID} of the corresponding
	 * output channel in the execution graph.
	 * 
	 * @param source
	 *        ID of the corresponding output channel
	 */
	public ManagementEdgeID(ChannelID source) {
		super();
		this.setID(source);
	}

	/**
	 * Converts the management edge ID into a {@link ChannelID}.
	 * 
	 * @return the corresponding channelID.
	 */
	public ChannelID toChannelID() {

		final ChannelID channelID = new ChannelID();
		channelID.setID(this);

		return channelID;
	}
}
