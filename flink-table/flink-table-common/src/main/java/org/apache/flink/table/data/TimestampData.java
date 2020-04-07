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

package org.apache.flink.table.data;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.Preconditions;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * {@link TimestampData} is an internal data structure represents data of {@link TimestampType}
 * and {@link LocalZonedTimestampType} in Flink Table/SQL.
 *
 * <p>It is an immutable implementation which is composite of a millisecond
 * and nanoOfMillisecond since {@code 1970-01-01 00:00:00}.
 */
@PublicEvolving
public final class TimestampData implements Comparable<TimestampData> {

	private static final long serialVersionUID = 1L;

	// the number of milliseconds in a day
	private static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

	// this field holds the integral second and the milli-of-second
	private final long millisecond;

	// this field holds the nano-of-millisecond
	private final int nanoOfMillisecond;

	private TimestampData(long millisecond, int nanoOfMillisecond) {
		Preconditions.checkArgument(nanoOfMillisecond >= 0 && nanoOfMillisecond <= 999_999);
		this.millisecond = millisecond;
		this.nanoOfMillisecond = nanoOfMillisecond;
	}

	/**
	 * Gets the number of milli-seconds since {@code 1970-01-01 00:00:00}.
	 */
	public long getMillisecond() {
		return millisecond;
	}

	/**
	 * Gets the number of nanoseconds the nanosecond within the millisecond.
	 * The value range is from 0 to 999,999.
	 */
	public int getNanoOfMillisecond() {
		return nanoOfMillisecond;
	}

	/**
	 * Converts this {@code TimestampData} object to a {@link Timestamp}.
	 *
	 * @return An instance of {@link Timestamp}
	 */
	public Timestamp toTimestamp() {
		return Timestamp.valueOf(toLocalDateTime());
	}

	/**
	 * Convert this {@code TimestampData} object to a {@link LocalDateTime}.
	 *
	 * @return An instance of {@link LocalDateTime}
	 */
	public LocalDateTime toLocalDateTime() {
		int date = (int) (millisecond / MILLIS_PER_DAY);
		int time = (int) (millisecond % MILLIS_PER_DAY);
		if (time < 0) {
			--date;
			time += MILLIS_PER_DAY;
		}
		long nanoOfDay = time * 1_000_000L + nanoOfMillisecond;
		LocalDate localDate = LocalDate.ofEpochDay(date);
		LocalTime localTime = LocalTime.ofNanoOfDay(nanoOfDay);
		return LocalDateTime.of(localDate, localTime);
	}

	/**
	 * Convert this {@code TimestampData} object to a {@link Instant}.
	 *
	 * @return an instance of {@link Instant}
	 */
	public Instant toInstant() {
		long epochSecond = millisecond / 1000;
		int milliOfSecond = (int) (millisecond % 1000);
		if (milliOfSecond < 0) {
			--epochSecond;
			milliOfSecond += 1000;
		}
		long nanoAdjustment = milliOfSecond * 1_000_000 + nanoOfMillisecond;
		return Instant.ofEpochSecond(epochSecond, nanoAdjustment);
	}

	@Override
	public int compareTo(TimestampData that) {
		int cmp = Long.compare(this.millisecond, that.millisecond);
		if (cmp == 0) {
			cmp = this.nanoOfMillisecond - that.nanoOfMillisecond;
		}
		return cmp;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof TimestampData)) {
			return false;
		}
		TimestampData that = (TimestampData) obj;
		return this.millisecond == that.millisecond &&
			this.nanoOfMillisecond == that.nanoOfMillisecond;
	}

	@Override
	public String toString() {
		return toLocalDateTime().toString();
	}

	@Override
	public int hashCode() {
		int ret = (int) millisecond ^ (int) (millisecond >> 32);
		return 31 * ret + nanoOfMillisecond;
	}

	// ------------------------------------------------------------------------------------------
	// Construct Utilities
	// ------------------------------------------------------------------------------------------

	/**
	 * Obtains an instance of {@code TimestampData} from a millisecond.
	 *
	 * <p>This returns a {@code TimestampData} with the specified millisecond.
	 * The nanoOfMillisecond field will be set to zero.
	 *
	 * @param millisecond the number of milliseconds since {@code 1970-01-01 00:00:00}
	 *                    A negative number is the number of milliseconds before
	 *                    {@code 1970-01-01 00:00:00}
	 * @return an instance of {@code TimestampData}
	 */
	public static TimestampData fromEpochMillis(long millisecond) {
		return new TimestampData(millisecond, 0);
	}

	/**
	 * Obtains an instance of {@code TimestampData} from a millisecond and a nanoOfMillisecond.
	 *
	 * <p>This returns a {@code TimestampData} with the specified millisecond and nanoOfMillisecond.
	 *
	 * @param millisecond the number of milliseconds since {@code 1970-01-01 00:00:00}
	 *                    A negative number is the number of milliseconds before
	 *                    {@code 1970-01-01 00:00:00}
	 * @param nanoOfMillisecond the nanosecond within the millisecond, from 0 to 999,999
	 * @return an instance of {@code TimestampData}
	 */
	public static TimestampData fromEpochMillis(long millisecond, int nanoOfMillisecond) {
		return new TimestampData(millisecond, nanoOfMillisecond);
	}

	/**
	 * Obtains an instance of {@code TimestampData} from an instance of {@link LocalDateTime}.
	 *
	 * <p>This returns a {@code TimestampData} with the specified {@link LocalDateTime}.
	 *
	 * @param dateTime an instance of {@link LocalDateTime}
	 * @return an instance of {@code TimestampData}
	 */
	public static TimestampData fromLocalDateTime(LocalDateTime dateTime) {
		long epochDay = dateTime.toLocalDate().toEpochDay();
		long nanoOfDay = dateTime.toLocalTime().toNanoOfDay();

		long millisecond = epochDay * MILLIS_PER_DAY + nanoOfDay / 1_000_000;
		int nanoOfMillisecond = (int) (nanoOfDay % 1_000_000);

		return new TimestampData(millisecond, nanoOfMillisecond);
	}

	/**
	 * Obtains an instance of {@code TimestampData} from an instance of {@link Timestamp}.
	 *
	 * <p>This returns a {@code TimestampData} with the specified {@link Timestamp}.
	 *
	 * @param ts an instance of {@link Timestamp}
	 * @return an instance of {@code TimestampData}
	 */
	public static TimestampData fromTimestamp(Timestamp ts) {
		return fromLocalDateTime(ts.toLocalDateTime());
	}

	/**
	 * Obtains an instance of {@code TimestampData} from an instance of {@link Instant}.
	 *
	 * <p>This returns a {@code TimestampData} with the specified {@link Instant}.
	 *
	 * @param instant an instance of {@link Instant}
	 * @return an instance of {@code TimestampData}
	 */
	public static TimestampData fromInstant(Instant instant) {
		long epochSecond = instant.getEpochSecond();
		int nanoSecond = instant.getNano();

		long millisecond = epochSecond * 1_000 + nanoSecond / 1_000_000;
		int nanoOfMillisecond = nanoSecond % 1_000_000;

		return new TimestampData(millisecond, nanoOfMillisecond);
	}

	/**
	 * Returns whether the timestamp data is small enough to be stored in a long of millisecond.
	 */
	public static boolean isCompact(int precision) {
		return precision <= 3;
	}
}
