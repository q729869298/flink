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

package org.apache.flink.stormcompatibility.wrappers;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.runtime.io.IndexedReaderIterator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.runtime.tasks.StreamTaskContext;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Fields;





@RunWith(PowerMockRunner.class)
@PrepareForTest({StreamRecordSerializer.class, StormWrapperSetupHelper.class})
public class StormBoltWrapperTest {
	
	@SuppressWarnings("unused")
	@Test(expected = IllegalArgumentException.class)
	public void testWrapperRawType() throws Exception {
		final StormOutputFieldsDeclarer declarer = new StormOutputFieldsDeclarer();
		declarer.declare(new Fields("dummy1", "dummy2"));
		PowerMockito.whenNew(StormOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);
		
		new StormBoltWrapper<Object, Object>(mock(IRichBolt.class), true);
	}
	
	@SuppressWarnings("unused")
	@Test(expected = IllegalArgumentException.class)
	public void testWrapperToManyAttributes1() throws Exception {
		final StormOutputFieldsDeclarer declarer = new StormOutputFieldsDeclarer();
		final String[] schema = new String[26];
		for(int i = 0; i < schema.length; ++i) {
			schema[i] = "a" + i;
		}
		declarer.declare(new Fields(schema));
		PowerMockito.whenNew(StormOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);
		
		new StormBoltWrapper<Object, Object>(mock(IRichBolt.class));
	}
	
	@SuppressWarnings("unused")
	@Test(expected = IllegalArgumentException.class)
	public void testWrapperToManyAttributes2() throws Exception {
		final StormOutputFieldsDeclarer declarer = new StormOutputFieldsDeclarer();
		final String[] schema = new String[26];
		for(int i = 0; i < schema.length; ++i) {
			schema[i] = "a" + i;
		}
		declarer.declare(new Fields(schema));
		PowerMockito.whenNew(StormOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);
		
		new StormBoltWrapper<Object, Object>(mock(IRichBolt.class), false);
	}
	
	@Test
	public void testWrapper() throws Exception {
		for(int i = 0; i < 26; ++i) {
			this.testWrapper(i);
		}
	}
	
	@SuppressWarnings({"rawtypes", "unchecked"})
	private void testWrapper(final int numberOfAttributes) throws Exception {
		assert ((0 <= numberOfAttributes) && (numberOfAttributes <= 25));
		
		Tuple flinkTuple = null;
		String rawTuple = null;
		
		if(numberOfAttributes == 0) {
			rawTuple = new String("test");
		} else {
			flinkTuple = Tuple.getTupleClass(numberOfAttributes).newInstance();
		}
		
		String[] schema = new String[numberOfAttributes];
		if(numberOfAttributes == 0) {
			schema = new String[1];
		}
		for(int i = 0; i < schema.length; ++i) {
			schema[i] = "a" + i;
		}
		
		
		final StreamRecord record = mock(StreamRecord.class);
		if(numberOfAttributes == 0) {
			when(record.getObject()).thenReturn(rawTuple);
		} else {
			when(record.getObject()).thenReturn(flinkTuple);
		}
		
		final StreamRecordSerializer serializer = mock(StreamRecordSerializer.class);
		when(serializer.createInstance()).thenReturn(record);
		
		final IndexedReaderIterator reader = mock(IndexedReaderIterator.class);
		when(reader.next(record)).thenReturn(record).thenReturn(null);
		
		final StreamTaskContext taskContext = mock(StreamTaskContext.class);
		when(taskContext.getInputSerializer(0)).thenReturn(serializer);
		when(taskContext.getIndexedInput(0)).thenReturn(reader);
		
		
		
		final IRichBolt bolt = mock(IRichBolt.class);
		
		final StormOutputFieldsDeclarer declarer = new StormOutputFieldsDeclarer();
		declarer.declare(new Fields(schema));
		PowerMockito.whenNew(StormOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);
		
		final StormBoltWrapper wrapper = new StormBoltWrapper(bolt);
		wrapper.setup(taskContext);
		
		
		
		wrapper.callUserFunction();
		if(numberOfAttributes == 0) {
			verify(bolt).execute(eq(new StormTuple<String>(rawTuple)));
		} else {
			verify(bolt).execute(eq(new StormTuple<Tuple>(flinkTuple)));
		}
		
		
		
		wrapper.run();
		if(numberOfAttributes == 0) {
			verify(bolt, times(2)).execute(eq(new StormTuple<String>(rawTuple)));
		} else {
			verify(bolt, times(2)).execute(eq(new StormTuple<Tuple>(flinkTuple)));
		}
	}
	
	@Test
	public void testOpen() throws Exception {
		final IRichBolt bolt = mock(IRichBolt.class);
		
		final StormOutputFieldsDeclarer declarer = new StormOutputFieldsDeclarer();
		declarer.declare(new Fields("dummy"));
		PowerMockito.whenNew(StormOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);
		
		final StormBoltWrapper<Object, Object> wrapper = new StormBoltWrapper<Object, Object>(bolt);
		wrapper.setRuntimeContext(mock(StreamingRuntimeContext.class));
		
		wrapper.open(mock(Configuration.class));
		
		verify(bolt).prepare(any(Map.class), any(TopologyContext.class), any(OutputCollector.class));
	}
	
	@Test
	public void testOpenSink() throws Exception {
		final IRichBolt bolt = mock(IRichBolt.class);
		final StormBoltWrapper<Object, Object> wrapper = new StormBoltWrapper<Object, Object>(bolt);
		wrapper.setRuntimeContext(mock(StreamingRuntimeContext.class));
		
		wrapper.open(mock(Configuration.class));
		
		verify(bolt).prepare(any(Map.class), any(TopologyContext.class), isNull(OutputCollector.class));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testClose() throws Exception {
		final IRichBolt bolt = mock(IRichBolt.class);
		
		final StormOutputFieldsDeclarer declarer = new StormOutputFieldsDeclarer();
		declarer.declare(new Fields("dummy"));
		PowerMockito.whenNew(StormOutputFieldsDeclarer.class).withNoArguments().thenReturn(declarer);
		
		final StormBoltWrapper<Object, Object> wrapper = new StormBoltWrapper<Object, Object>(bolt);
		
		final StreamTaskContext<Object> taskContext = mock(StreamTaskContext.class);
		when(taskContext.getOutputCollector()).thenReturn(mock(Collector.class));
		wrapper.setup(taskContext);
		
		wrapper.close();
		verify(bolt).cleanup();
	}
	
}
