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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.io.network.channels.ChannelID;

public class ExecutionEdge {

	private final IntermediateResultPartition source;
	
	private final ExecutionVertex target;
	
	private final int inputNum;

	private ChannelID inputChannelId;
	
	private ChannelID outputChannelId;
	
	
	public ExecutionEdge(IntermediateResultPartition source, ExecutionVertex target, int inputNum) {
		this.source = source;
		this.target = target;
		this.inputNum = inputNum;
		
		this.inputChannelId = new ChannelID();
		this.outputChannelId = new ChannelID();
	}
	
	
	public IntermediateResultPartition getSource() {
		return source;
	}
	
	public ExecutionVertex getTarget() {
		return target;
	}
	
	public int getInputNum() {
		return inputNum;
	}
	
	public ChannelID getInputChannelId() {
		return inputChannelId;
	}
	
	public ChannelID getOutputChannelId() {
		return outputChannelId;
	}
	
	public void assignNewChannelIDs() {
		inputChannelId = new ChannelID();
		outputChannelId = new ChannelID();
	}
}
