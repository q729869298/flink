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

package org.apache.flink.table.runtime.window;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.window.TimeWindow;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import static org.apache.flink.table.dataformat.BinaryString.fromString;

/**
 * Utilities that are useful for working with Window tests.
 */
public interface WindowTestUtils {

	static Matcher<TimeWindow> timeWindow(long start, long end) {
		return Matchers.equalTo(new TimeWindow(start, end));
	}

	static StreamRecord<BaseRow> record(String key, Object... fields) {
		return new StreamRecord<>(baserow(key, fields));
	}

	static StreamRecord<BaseRow> retractRecord(String key, Object... fields) {
		BaseRow row = baserow(key, fields);
		BaseRowUtil.setRetract(row);
		return new StreamRecord<>(row);
	}

	static BaseRow baserow(String key, Object... fields) {
		Object[] objects = new Object[fields.length + 1];
		objects[0] = fromString(key);
		System.arraycopy(fields, 0, objects, 1, fields.length);
		return GenericRow.of(objects);
	}
}
