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

package org.apache.flink.table.runtime.operators.window.slicing;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SliceAssigners.HoppingSliceAssigner}. */
@RunWith(Parameterized.class)
public class HoppingSliceAssignerTest extends SliceAssignerTestBase {

    @Parameterized.Parameter public ZoneId shiftTimeZone;

    @Parameterized.Parameters(name = "timezone = {0}")
    public static Collection<ZoneId> parameters() {
        return Arrays.asList(ZoneId.of("America/Los_Angeles"), ZoneId.of("Asia/Shanghai"));
    }

    @Test
    public void testSliceAssignment() {
        SliceAssigner assigner =
                SliceAssigners.hopping(0, shiftTimeZone, Duration.ofHours(5), Duration.ofHours(1));

        assertThat(assignSliceEnd(assigner, localMills("1970-01-01T00:00:00")))
                .isEqualTo(utcMills("1970-01-01T01:00:00"));
        assertThat(assignSliceEnd(assigner, localMills("1970-01-01T04:59:59.999")))
                .isEqualTo(utcMills("1970-01-01T05:00:00"));
        assertThat(assignSliceEnd(assigner, localMills("1970-01-01T05:00:00")))
                .isEqualTo(utcMills("1970-01-01T06:00:00"));
    }

    @Test
    public void testSliceAssignmentWithOffset() {
        SliceAssigner assigner =
                SliceAssigners.hopping(0, shiftTimeZone, Duration.ofHours(5), Duration.ofHours(1))
                        .withOffset(Duration.ofMillis(100));

        assertThat(assignSliceEnd(assigner, localMills("1970-01-01T00:00:00.1")))
                .isEqualTo(utcMills("1970-01-01T01:00:00.1"));
        assertThat(assignSliceEnd(assigner, localMills("1970-01-01T05:00:00.099")))
                .isEqualTo(utcMills("1970-01-01T05:00:00.1"));
        assertThat(assignSliceEnd(assigner, localMills("1970-01-01T05:00:00.1")))
                .isEqualTo(utcMills("1970-01-01T06:00:00.1"));
    }

    @Test
    public void testDstSaving() {
        if (!TimeZone.getTimeZone(shiftTimeZone).useDaylightTime()) {
            return;
        }
        SliceAssigner assigner =
                SliceAssigners.hopping(0, shiftTimeZone, Duration.ofHours(4), Duration.ofHours(1));

        // Los_Angeles local time in epoch mills.
        // The DaylightTime in Los_Angele start at time 2021-03-14 02:00:00
        long epoch1 = 1615708800000L; // 2021-03-14 00:00:00
        long epoch2 = 1615712400000L; // 2021-03-14 01:00:00
        long epoch3 = 1615716000000L; // 2021-03-14 03:00:00, skip one hour (2021-03-14 02:00:00)
        long epoch4 = 1615719600000L; // 2021-03-14 04:00:00

        assertSliceStartEnd("2021-03-13T21:00", "2021-03-14T01:00", epoch1, assigner);
        assertSliceStartEnd("2021-03-13T22:00", "2021-03-14T02:00", epoch2, assigner);
        assertSliceStartEnd("2021-03-14T00:00", "2021-03-14T04:00", epoch3, assigner);
        assertSliceStartEnd("2021-03-14T01:00", "2021-03-14T05:00", epoch4, assigner);

        // Los_Angeles local time in epoch mills.
        // The DaylightTime in Los_Angele end at time 2021-11-07 02:00:00
        long epoch5 = 1636268400000L; // 2021-11-07 00:00:00
        long epoch6 = 1636272000000L; // the first local timestamp 2021-11-07 01:00:00
        long epoch7 = 1636275600000L; // rollback to  2021-11-07 01:00:00
        long epoch8 = 1636279200000L; // 2021-11-07 02:00:00
        long epoch9 = 1636282800000L; // 2021-11-07 03:00:00
        long epoch10 = 1636286400000L; // 2021-11-07 04:00:00

        assertSliceStartEnd("2021-11-06T21:00", "2021-11-07T01:00", epoch5, assigner);
        assertSliceStartEnd("2021-11-06T22:00", "2021-11-07T02:00", epoch6, assigner);
        assertSliceStartEnd("2021-11-06T22:00", "2021-11-07T02:00", epoch7, assigner);
        assertSliceStartEnd("2021-11-06T23:00", "2021-11-07T03:00", epoch8, assigner);
        assertSliceStartEnd("2021-11-07T00:00", "2021-11-07T04:00", epoch9, assigner);
        assertSliceStartEnd("2021-11-07T01:00", "2021-11-07T05:00", epoch10, assigner);
    }

    @Test
    public void testGetWindowStart() {
        SliceAssigner assigner =
                SliceAssigners.hopping(0, shiftTimeZone, Duration.ofHours(5), Duration.ofHours(1));

        assertThat(assigner.getWindowStart(utcMills("1970-01-01T00:00:00")))
                .isEqualTo(utcMills("1969-12-31T19:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T01:00:00")))
                .isEqualTo(utcMills("1969-12-31T20:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T02:00:00")))
                .isEqualTo(utcMills("1969-12-31T21:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T03:00:00")))
                .isEqualTo(utcMills("1969-12-31T22:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T04:00:00")))
                .isEqualTo(utcMills("1969-12-31T23:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T05:00:00")))
                .isEqualTo(utcMills("1970-01-01T00:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T06:00:00")))
                .isEqualTo(utcMills("1970-01-01T01:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T10:00:00")))
                .isEqualTo(utcMills("1970-01-01T05:00:00"));
    }

    @Test
    public void testExpiredSlices() {

        SliceAssigner assigner =
                SliceAssigners.hopping(0, shiftTimeZone, Duration.ofHours(4), Duration.ofHours(1));

        assertThat(expiredSlices(assigner, utcMills("1970-01-01T00:00:00")))
                .containsExactly(utcMills("1969-12-31T21:00:00"));
        assertThat(expiredSlices(assigner, utcMills("1970-01-01T04:00:00")))
                .containsExactly(utcMills("1970-01-01T01:00:00"));
        assertThat(expiredSlices(assigner, utcMills("1970-01-01T08:00:00")))
                .containsExactly(utcMills("1970-01-01T05:00:00"));
    }

    @Test
    public void testMerge() throws Exception {
        SliceAssigners.HoppingSliceAssigner assigner =
                SliceAssigners.hopping(0, shiftTimeZone, Duration.ofHours(5), Duration.ofHours(1));

        assertThat(mergeResultSlice(assigner, utcMills("1970-01-01T00:00:00"))).isNull();
        assertThat(toBeMergedSlices(assigner, utcMills("1970-01-01T00:00:00")))
                .isEqualTo(
                        Arrays.asList(
                                utcMills("1970-01-01T00:00:00"),
                                utcMills("1969-12-31T23:00:00"),
                                utcMills("1969-12-31T22:00:00"),
                                utcMills("1969-12-31T21:00:00"),
                                utcMills("1969-12-31T20:00:00")));

        assertThat(mergeResultSlice(assigner, utcMills("1970-01-01T05:00:00"))).isNull();
        assertThat(toBeMergedSlices(assigner, utcMills("1970-01-01T05:00:00")))
                .isEqualTo(
                        Arrays.asList(
                                utcMills("1970-01-01T05:00:00"),
                                utcMills("1970-01-01T04:00:00"),
                                utcMills("1970-01-01T03:00:00"),
                                utcMills("1970-01-01T02:00:00"),
                                utcMills("1970-01-01T01:00:00")));

        assertThat(mergeResultSlice(assigner, utcMills("1970-01-01T06:00:00"))).isNull();
        assertThat(toBeMergedSlices(assigner, utcMills("1970-01-01T06:00:00")))
                .isEqualTo(
                        Arrays.asList(
                                utcMills("1970-01-01T06:00:00"),
                                utcMills("1970-01-01T05:00:00"),
                                utcMills("1970-01-01T04:00:00"),
                                utcMills("1970-01-01T03:00:00"),
                                utcMills("1970-01-01T02:00:00")));
    }

    @Test
    public void testNextTriggerWindow() {
        SliceAssigners.HoppingSliceAssigner assigner =
                SliceAssigners.hopping(0, shiftTimeZone, Duration.ofHours(5), Duration.ofHours(1));

        assertThat(assigner.nextTriggerWindow(utcMills("1970-01-01T00:00:00"), () -> false))
                .isEqualTo(Optional.of(utcMills("1970-01-01T01:00:00")));
        assertThat(assigner.nextTriggerWindow(utcMills("1970-01-01T01:00:00"), () -> false))
                .isEqualTo(Optional.of(utcMills("1970-01-01T02:00:00")));
        assertThat(assigner.nextTriggerWindow(utcMills("1970-01-01T02:00:00"), () -> false))
                .isEqualTo(Optional.of(utcMills("1970-01-01T03:00:00")));
        assertThat(assigner.nextTriggerWindow(utcMills("1970-01-01T03:00:00"), () -> false))
                .isEqualTo(Optional.of(utcMills("1970-01-01T04:00:00")));
        assertThat(assigner.nextTriggerWindow(utcMills("1970-01-01T04:00:00"), () -> false))
                .isEqualTo(Optional.of(utcMills("1970-01-01T05:00:00")));
        assertThat(assigner.nextTriggerWindow(utcMills("1970-01-01T05:00:00"), () -> false))
                .isEqualTo(Optional.of(utcMills("1970-01-01T06:00:00")));
        assertThat(assigner.nextTriggerWindow(utcMills("1970-01-01T06:00:00"), () -> false))
                .isEqualTo(Optional.of(utcMills("1970-01-01T07:00:00")));

        assertThat(assigner.nextTriggerWindow(utcMills("1970-01-01T00:00:00"), () -> true))
                .isEqualTo(Optional.empty());
        assertThat(assigner.nextTriggerWindow(utcMills("1970-01-01T01:00:00"), () -> true))
                .isEqualTo(Optional.empty());
        assertThat(assigner.nextTriggerWindow(utcMills("1970-01-01T02:00:00"), () -> true))
                .isEqualTo(Optional.empty());
        assertThat(assigner.nextTriggerWindow(utcMills("1970-01-01T03:00:00"), () -> true))
                .isEqualTo(Optional.empty());
        assertThat(assigner.nextTriggerWindow(utcMills("1970-01-01T04:00:00"), () -> true))
                .isEqualTo(Optional.empty());
        assertThat(assigner.nextTriggerWindow(utcMills("1970-01-01T05:00:00"), () -> true))
                .isEqualTo(Optional.empty());
        assertThat(assigner.nextTriggerWindow(utcMills("1970-01-01T06:00:00"), () -> true))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testEventTime() {
        SliceAssigner assigner1 =
                SliceAssigners.hopping(
                        0, shiftTimeZone, Duration.ofSeconds(5), Duration.ofSeconds(1));
        assertThat(assigner1.isEventTime()).isTrue();

        SliceAssigner assigner2 =
                SliceAssigners.hopping(
                        -1, shiftTimeZone, Duration.ofSeconds(5), Duration.ofSeconds(1));
        assertThat(assigner2.isEventTime()).isFalse();
    }

    @Test
    public void testInvalidParameters() {
        assertErrorMessage(
                () ->
                        SliceAssigners.hopping(
                                0, shiftTimeZone, Duration.ofSeconds(-2), Duration.ofSeconds(1)),
                "Hopping Window must satisfy slide > 0 and size > 0, but got slide 1000ms and size -2000ms.");

        assertErrorMessage(
                () ->
                        SliceAssigners.hopping(
                                0, shiftTimeZone, Duration.ofSeconds(2), Duration.ofSeconds(-1)),
                "Hopping Window must satisfy slide > 0 and size > 0, but got slide -1000ms and size 2000ms.");

        assertErrorMessage(
                () ->
                        SliceAssigners.hopping(
                                0, shiftTimeZone, Duration.ofSeconds(5), Duration.ofSeconds(2)),
                "Slicing Hopping Window requires size must be an integral multiple of slide, but got size 5000ms and slide 2000ms.");

        // should pass
        SliceAssigners.hopping(0, shiftTimeZone, Duration.ofSeconds(10), Duration.ofSeconds(5))
                .withOffset(Duration.ofSeconds(-1));
    }

    private long localMills(String timestampStr) {
        return localMills(timestampStr, shiftTimeZone);
    }
}
