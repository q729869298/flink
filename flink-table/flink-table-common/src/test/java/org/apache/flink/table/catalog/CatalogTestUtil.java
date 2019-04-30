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

package org.apache.flink.table.catalog;

<<<<<<< HEAD:flink-table/flink-table-common/src/test/java/org/apache/flink/table/catalog/CatalogTestUtil.java
=======
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

>>>>>>> Reworked stats related classes and APIs to address some of the review comments:flink-table/flink-table-api-java/src/test/java/org/apache/flink/table/catalog/CatalogTestUtil.java
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Utility class for catalog testing.
 */
public class CatalogTestUtil {

	public static void checkEquals(CatalogTable t1, CatalogTable t2) {
		assertEquals(t1.getSchema(), t2.getSchema());
		assertEquals(t1.getComment(), t2.getComment());
		assertEquals(t1.getProperties(), t2.getProperties());
		assertEquals(t1.getPartitionKeys(), t2.getPartitionKeys());
		assertEquals(t1.isPartitioned(), t2.isPartitioned());
		assertEquals(t1.getDescription(), t2.getDescription());
	}

	public static void checkEquals(CatalogView v1, CatalogView v2) {
		assertEquals(v1.getSchema(), v1.getSchema());
		assertEquals(v1.getProperties(), v2.getProperties());
		assertEquals(v1.getComment(), v2.getComment());
		assertEquals(v1.getOriginalQuery(), v2.getOriginalQuery());
		assertEquals(v1.getExpandedQuery(), v2.getExpandedQuery());
	}

	public static void checkEquals(CatalogDatabase d1, CatalogDatabase d2) {
		assertEquals(d1.getProperties(), d2.getProperties());
	}

	public static void checkEquals(CatalogFunction f1, CatalogFunction f2) {
		assertEquals(f1.getClassName(), f2.getClassName());
		assertEquals(f1.getProperties(), f2.getProperties());
	}

	public static void checkEquals(CatalogPartition p1, CatalogPartition p2) {
		assertEquals(p1.getProperties(), p2.getProperties());
	}

	static void checkEquals(CatalogTableStatistics ts1, CatalogTableStatistics ts2) {
		assertEquals(ts1.getRowCount(), ts2.getRowCount());
		assertEquals(ts1.getFileCount(), ts2.getFileCount());
		assertEquals(ts1.getTotalSize(), ts2.getTotalSize());
		assertEquals(ts1.getRawDataSize(), ts2.getRawDataSize());
		assertEquals(ts1.getProperties(), ts2.getProperties());
	}

	static void checkEquals(CatalogColumnStatistics cs1, CatalogColumnStatistics cs2) {
		checkEquals(cs1.getColumnStatisticsData(), cs2.getColumnStatisticsData());
		assertEquals(cs1.getProperties(), cs2.getProperties());
	}

	private static void checkEquals(Map<String, CatalogColumnStatisticsDataBase> m1, Map<String, CatalogColumnStatisticsDataBase> m2) {
		assertEquals(m1.size(), m2.size());
		for (Map.Entry<String, CatalogColumnStatisticsDataBase> entry : m2.entrySet()) {
			assertTrue(m1.containsKey(entry.getKey()));
			checkEquals(m2.get(entry.getKey()), entry.getValue());
		}
	}

	private static void checkEquals(CatalogColumnStatisticsDataBase v1, CatalogColumnStatisticsDataBase v2) {
		assertEquals(v1.getClass(), v2.getClass());
		if (v1 instanceof CatalogColumnStatisticsDataBoolean) {
			checkEquals((CatalogColumnStatisticsDataBoolean) v1, (CatalogColumnStatisticsDataBoolean) v2);
		} else if (v1 instanceof CatalogColumnStatisticsDataLong) {
			checkEquals((CatalogColumnStatisticsDataLong) v1, (CatalogColumnStatisticsDataLong) v2);
		}
	}

	private static void checkEquals(CatalogColumnStatisticsDataBoolean v1, CatalogColumnStatisticsDataBoolean v2) {
		assertEquals(v1.getFalseCount(), v2.getFalseCount());
		assertEquals(v1.getTrueCount(), v2.getTrueCount());
		assertEquals(v1.getNullCount(), v2.getNullCount());
		assertEquals(v1.getProperties(), v2.getProperties());
	}

	private static void checkEquals(CatalogColumnStatisticsDataLong v1, CatalogColumnStatisticsDataLong v2) {
		assertEquals(v1.getMin(), v2.getMin());
		assertEquals(v1.getMax(), v2.getMax());
		assertEquals(v1.getNdv(), v2.getNdv());
		assertEquals(v1.getNullCount(), v2.getNullCount());
		assertEquals(v1.getProperties(), v2.getProperties());
	}

}
