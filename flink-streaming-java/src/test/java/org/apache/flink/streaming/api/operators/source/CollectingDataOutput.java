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

package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;

import java.util.ArrayList;
import java.util.List;

/**
 * A test utility implementation of {@link PushingAsyncDataInput.DataOutput} that collects all events.
 */
public final class CollectingDataOutput<E> implements PushingAsyncDataInput.DataOutput<E> {

	public final List<Object> events = new ArrayList<>();

	@Override
	public void emitWatermark(Watermark watermark) throws Exception {
		events.add(watermark);
	}

	@Override
	public void emitStreamStatus(StreamStatus streamStatus) throws Exception {
		events.add(streamStatus);
	}

	@Override
	public void emitRecord(StreamRecord<E> streamRecord) throws Exception {
		events.add(streamRecord);
	}

	@Override
	public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
		events.add(latencyMarker);
	}
}
