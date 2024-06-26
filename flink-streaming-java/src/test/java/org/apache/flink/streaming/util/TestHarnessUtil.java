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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.eventtime.TimestampWatermark;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.WatermarkEvent;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Utils for working with the various test harnesses. */
public class TestHarnessUtil {

    /** Extracts the raw elements from the given output list. */
    @SuppressWarnings("unchecked")
    public static <OUT> List<OUT> getRawElementsFromOutput(Queue<Object> output) {
        List<OUT> resultElements = new LinkedList<>();
        for (Object e : output) {
            if (e instanceof StreamRecord) {
                resultElements.add(((StreamRecord<OUT>) e).getValue());
            }
        }
        return resultElements;
    }

    /**
     * Compare the two queues containing operator/task output by converting them to an array first.
     */
    public static <T> void assertOutputEquals(String message, Queue<T> expected, Queue<T> actual) {
        assertThat(actual.toArray()).as(message).isEqualTo(expected.toArray());
    }

    /**
     * Compare the two queues containing operator/task output by converting them to an array first.
     */
    public static void assertOutputEqualsSorted(
            String message,
            Iterable<Object> expected,
            Iterable<Object> actual,
            Comparator<Object> comparator) {
        assertThat(actual).hasSameSizeAs(expected);

        // first, compare only watermarks, their position should be deterministic
        Iterator<Object> exIt = expected.iterator();
        Iterator<Object> actIt = actual.iterator();
        while (exIt.hasNext()) {
            Object nextEx = exIt.next();
            Object nextAct = actIt.next();
            if (nextEx instanceof WatermarkEvent) {
                assertThat(nextAct).isEqualTo(nextEx);
            }
        }

        List<Object> expectedRecords = new ArrayList<>();
        List<Object> actualRecords = new ArrayList<>();

        for (Object ex : expected) {
            if (ex instanceof StreamRecord) {
                expectedRecords.add(ex);
            }
        }

        for (Object act : actual) {
            if (act instanceof StreamRecord) {
                actualRecords.add(act);
            }
        }

        Object[] sortedExpected = expectedRecords.toArray();
        Object[] sortedActual = actualRecords.toArray();

        Arrays.sort(sortedExpected, comparator);
        Arrays.sort(sortedActual, comparator);

        assertThat(sortedActual).as(message).isEqualTo(sortedExpected);
    }

    /**
     * Verify no StreamRecord is equal to or later than any watermarks. This is checked over the
     * order of the elements
     *
     * @param elements An iterable containing StreamRecords and watermarks
     */
    public static void assertNoLateRecords(Iterable<Object> elements) {
        // check that no watermark is violated
        long highestWatermark = Long.MIN_VALUE;

        for (Object elem : elements) {
            if (elem instanceof WatermarkEvent) {
                Watermark genericWatermark = ((WatermarkEvent) elem).asWatermark().getWatermark();
                assertThat(genericWatermark).isInstanceOf(TimestampWatermark.class);
                highestWatermark = ((TimestampWatermark) genericWatermark).getTimestamp();
            } else if (elem instanceof StreamRecord) {
                boolean dataIsOnTime = highestWatermark < ((StreamRecord) elem).getTimestamp();
                assertThat(dataIsOnTime).as("Late data was emitted after join").isTrue();
            }
        }
    }

    /**
     * Get the operator's state after processing given inputs.
     *
     * @param testHarness A operator whose state is computed
     * @param input A list of inputs
     * @return The operator's snapshot
     */
    public static <InputT, CommT> OperatorSubtaskState buildSubtaskState(
            OneInputStreamOperatorTestHarness<InputT, CommT> testHarness, List<InputT> input)
            throws Exception {
        testHarness.initializeEmptyState();
        testHarness.open();

        testHarness.processElements(
                input.stream().map(StreamRecord::new).collect(Collectors.toList()));
        testHarness.prepareSnapshotPreBarrier(1);
        final OperatorSubtaskState operatorSubtaskState = testHarness.snapshot(1, 1);
        testHarness.close();

        return operatorSubtaskState;
    }
}
