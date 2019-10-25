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

package org.apache.flink.table.catalog.hive.client;

import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.Date;
import org.apache.flink.table.functions.hive.FlinkHiveUDFException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.thrift.TException;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Shim for Hive version 1.2.0.
 */
public class HiveShimV120 implements HiveShim {

	@Override
	public IMetaStoreClient getHiveMetastoreClient(HiveConf hiveConf) {
		try {
			Method method = RetryingMetaStoreClient.class.getMethod("getProxy", HiveConf.class);
			// getProxy is a static method
			return (IMetaStoreClient) method.invoke(null, (hiveConf));
		} catch (Exception ex) {
			throw new CatalogException("Failed to create Hive Metastore client", ex);
		}
	}

	@Override
	// 1.x client doesn't support filtering tables by type, so here we need to get all tables and filter by ourselves
	public List<String> getViews(IMetaStoreClient client, String databaseName) throws UnknownDBException, TException {
		// We don't have to use reflection here because client.getAllTables(String) is supposed to be there for
		// all versions.
		List<String> tableNames = client.getAllTables(databaseName);
		List<String> views = new ArrayList<>();
		for (String name : tableNames) {
			Table table = client.getTable(databaseName, name);
			String viewDef = table.getViewOriginalText();
			if (viewDef != null && !viewDef.isEmpty()) {
				views.add(table.getTableName());
			}
		}
		return views;
	}

	@Override
	public Function getFunction(IMetaStoreClient client, String dbName, String functionName) throws NoSuchObjectException, TException {
		try {
			// hive-1.x doesn't throw NoSuchObjectException if function doesn't exist, instead it throws a MetaException
			return client.getFunction(dbName, functionName);
		} catch (MetaException e) {
			// need to check the cause and message of this MetaException to decide whether it should actually be a NoSuchObjectException
			if (e.getCause() instanceof NoSuchObjectException) {
				throw (NoSuchObjectException) e.getCause();
			}
			if (e.getMessage().startsWith(NoSuchObjectException.class.getSimpleName())) {
				throw new NoSuchObjectException(e.getMessage());
			}
			throw e;
		}
	}

	@Override
	public boolean moveToTrash(FileSystem fs, Path path, Configuration conf, boolean purge) throws IOException {
		try {
			Method method = FileUtils.class.getDeclaredMethod("moveToTrash", FileSystem.class, Path.class, Configuration.class);
			return (boolean) method.invoke(null, fs, path, conf);
		} catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
			throw new IOException("Failed to move " + path + " to trash", e);
		}
	}

	@Override
	public void alterTable(IMetaStoreClient client, String databaseName, String tableName, Table table) throws InvalidOperationException, MetaException, TException {
		// For Hive-1.2.1, we need to tell HMS not to update stats. Otherwise, the stats we put in the table
		// parameters can be overridden. The extra config we add here will be removed by HMS after it's used.
		// Don't use StatsSetupConst.DO_NOT_UPDATE_STATS because it wasn't defined in Hive 1.1.x.
		table.getParameters().put("DO_NOT_UPDATE_STATS", "true");
		client.alter_table(databaseName, tableName, table);
	}

	@Override
	public void alterPartition(IMetaStoreClient client, String databaseName, String tableName, Partition partition)
			throws InvalidOperationException, MetaException, TException {
		String errorMsg = "Failed to alter partition for table %s in database %s";
		try {
			Method method = client.getClass().getMethod("alter_partition", String.class, String.class, Partition.class);
			method.invoke(client, databaseName, tableName, partition);
		} catch (InvocationTargetException ite) {
			Throwable targetEx = ite.getTargetException();
			if (targetEx instanceof TException) {
				throw (TException) targetEx;
			} else {
				throw new CatalogException(String.format(errorMsg, tableName, databaseName), targetEx);
			}
		} catch (NoSuchMethodException | IllegalAccessException e) {
			throw new CatalogException(String.format(errorMsg, tableName, databaseName), e);
		}
	}

	@Override
	public SimpleGenericUDAFParameterInfo createUDAFParameterInfo(ObjectInspector[] params, boolean isWindowing, boolean distinct, boolean allColumns) {
		try {
			Constructor constructor = SimpleGenericUDAFParameterInfo.class.getConstructor(ObjectInspector[].class,
					boolean.class, boolean.class);
			return (SimpleGenericUDAFParameterInfo) constructor.newInstance(params, distinct, allColumns);
		} catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
			throw new CatalogException("Failed to create SimpleGenericUDAFParameterInfo", e);
		}
	}

	@Override
	public Class<?> getMetaStoreUtilsClass() {
		try {
			return Class.forName("org.apache.hadoop.hive.metastore.MetaStoreUtils");
		} catch (ClassNotFoundException e) {
			throw new CatalogException("Failed to find class MetaStoreUtils", e);
		}
	}

	@Override
	public Class<?> getHiveMetaStoreUtilsClass() {
		return getMetaStoreUtilsClass();
	}

	@Override
	public Class<?> getDateDataTypeClass() {
		return java.sql.Date.class;
	}

	@Override
	public Class<?> getTimestampDataTypeClass() {
		return java.sql.Timestamp.class;
	}

	@Override
	public FileStatus[] getFileStatusRecurse(Path path, int level, FileSystem fs) throws IOException {
		try {
			Method method = HiveStatsUtils.class.getMethod("getFileStatusRecurse", Path.class, Integer.TYPE, FileSystem.class);
			// getFileStatusRecurse is a static method
			return (FileStatus[]) method.invoke(null, path, level, fs);
		} catch (Exception ex) {
			throw new CatalogException("Failed to invoke HiveStatsUtils.getFileStatusRecurse()", ex);
		}
	}

	@Override
	public void makeSpecFromName(Map<String, String> partSpec, Path currPath) {
		try {
			Method method = Warehouse.class.getMethod("makeSpecFromName", Map.class, Path.class);
			// makeSpecFromName is a static method
			method.invoke(null, partSpec, currPath);
		} catch (Exception ex) {
			throw new CatalogException("Failed to invoke Warehouse.makeSpecFromName()", ex);
		}
	}

	@Override
	public ObjectInspector getObjectInspectorForConstant(PrimitiveTypeInfo primitiveTypeInfo, Object value) {
		String className;
		switch (primitiveTypeInfo.getPrimitiveCategory()) {
			case BOOLEAN:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantBooleanObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case BYTE:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantByteObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case SHORT:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantShortObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case INT:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantIntObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case LONG:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantLongObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case FLOAT:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantFloatObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case DOUBLE:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantDoubleObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case STRING:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantStringObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case CHAR:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantHiveCharObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case VARCHAR:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantHiveVarcharObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case DATE:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantDateObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case TIMESTAMP:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantTimestampObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case DECIMAL:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantHiveDecimalObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case BINARY:
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantBinaryObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value);
			case UNKNOWN:
			case VOID:
				// If type is null, we use the Java Constant String to replace
				className = "org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantStringObjectInspector";
				return HiveReflectionUtils.createConstantObjectInspector(className, value.toString());
			default:
				throw new FlinkHiveUDFException(
					String.format("Cannot find ConstantObjectInspector for %s", primitiveTypeInfo));
		}
	}

	@Override
	public ColumnStatisticsData toHiveDateColStats(CatalogColumnStatisticsDataDate flinkDateColStats) {
		try {
			Class dateStatsClz = Class.forName("org.apache.hadoop.hive.metastore.api.DateColumnStatsData");
			Object dateStats = dateStatsClz.getDeclaredConstructor(long.class, long.class)
					.newInstance(flinkDateColStats.getNullCount(), flinkDateColStats.getNdv());
			Class hmsDateClz = Class.forName("org.apache.hadoop.hive.metastore.api.Date");
			Method setHigh = dateStatsClz.getDeclaredMethod("setHighValue", hmsDateClz);
			Method setLow = dateStatsClz.getDeclaredMethod("setLowValue", hmsDateClz);
			Constructor hmsDateConstructor = hmsDateClz.getConstructor(long.class);
			setHigh.invoke(dateStats, hmsDateConstructor.newInstance(flinkDateColStats.getMax().getDaysSinceEpoch()));
			setLow.invoke(dateStats, hmsDateConstructor.newInstance(flinkDateColStats.getMin().getDaysSinceEpoch()));
			Class colStatsClz = ColumnStatisticsData.class;
			return (ColumnStatisticsData) colStatsClz.getDeclaredMethod("dateStats", dateStatsClz).invoke(null, dateStats);
		} catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
			throw new CatalogException("Failed to create Hive statistics for date column", e);
		}
	}

	@Override
	public boolean isDateStats(ColumnStatisticsData colStatsData) {
		try {
			Method method = ColumnStatisticsData.class.getDeclaredMethod("isSetDateStats");
			return (boolean) method.invoke(colStatsData);
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			throw new CatalogException("Failed to decide whether ColumnStatisticsData is for DATE column", e);
		}
	}

	@Override
	public CatalogColumnStatisticsDataDate toFlinkDateColStats(ColumnStatisticsData hiveDateColStats) {
		try {
			Object dateStats = ColumnStatisticsData.class.getDeclaredMethod("getDateStats").invoke(hiveDateColStats);
			Class dateStatsClz = dateStats.getClass();
			long numDV = (long) dateStatsClz.getDeclaredMethod("getNumDVs").invoke(dateStats);
			long numNull = (long) dateStatsClz.getDeclaredMethod("getNumNulls").invoke(dateStats);
			Object hmsHighDate = dateStatsClz.getDeclaredMethod("getHighValue").invoke(dateStats);
			Object hmsLowDate = dateStatsClz.getDeclaredMethod("getLowValue").invoke(dateStats);
			Class hmsDateClz = hmsHighDate.getClass();
			Method hmsDateDays = hmsDateClz.getDeclaredMethod("getDaysSinceEpoch");
			long highDateDays = (long) hmsDateDays.invoke(hmsHighDate);
			long lowDateDays = (long) hmsDateDays.invoke(hmsLowDate);
			return new CatalogColumnStatisticsDataDate(new Date(lowDateDays), new Date(highDateDays), numDV, numNull);
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			throw new CatalogException("Failed to create Flink statistics for date column", e);
		}
	}
}
