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
package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.util.TestReporter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AbstractMetricGroupTest {
	/**
	 * Verifies that no {@link NullPointerException} is thrown when {@link AbstractMetricGroup#getAllVariables()} is
	 * called and the parent is null.
	 */
	@Test
	public void testGetAllVariables() {
		MetricRegistry registry = new MetricRegistry(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());

		AbstractMetricGroup group = new AbstractMetricGroup<AbstractMetricGroup<?>>(registry, new String[0], null) {
			@Override
			protected QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
				return null;
			}
		};
		assertTrue(group.getAllVariables().isEmpty());
		
		registry.shutdown();
	}

	// for test case: one filter for different reporters with different of scope delimiter
	protected static CharacterFilter staticCharacterFilter = new CharacterFilter() {
		@Override
		public String filterCharacters(String input) {
			return input.replace("C", "RR");
		}
	};

	@Test
	public void filteringForMultipleReporters() {
		TestReporter1.countSuccessChecks = 0;
		Configuration config = new Configuration();
		config.setString(ConfigConstants.METRICS_SCOPE_NAMING_TM, "A.B.C.D");
		config.setString(ConfigConstants.METRICS_REPORTERS_LIST, "test1,test2");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter1.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, TestReporter2.class.getName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, "-");
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test2." + ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, "!");


		MetricRegistry testRegistry = new MetricRegistryTest(MetricRegistryConfiguration.fromConfiguration(config));
		TaskManagerMetricGroup tmGroup = new TaskManagerMetricGroup(testRegistry, "host", "id");
		tmGroup.counter(1);
		testRegistry.shutdown();
		assert TestReporter1.countSuccessChecks == 4;
	}

	@Test
	public void filteringForNullReporters() {
		MetricRegistryTest.countSuccessChecks = 0;
		Configuration config = new Configuration();
		config.setString(ConfigConstants.METRICS_SCOPE_NAMING_TM, "A.B.C.D");
		MetricRegistry testRegistry = new MetricRegistryTest(MetricRegistryConfiguration.fromConfiguration(config));
		TaskManagerMetricGroup tmGroupForTestRegistry = new TaskManagerMetricGroup(testRegistry, "host", "id");
		assert testRegistry.getReporters().size() == 0;
		tmGroupForTestRegistry.counter(1);
		testRegistry.shutdown();
		assert MetricRegistryTest.countSuccessChecks == 1;
	}

	public static class TestReporter1 extends TestReporter {
		protected static int countSuccessChecks = 0;
		@Override
		public String filterCharacters(String input) {
			return input.replace("A", "RR");
		}
		@Override
		public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
			assertEquals("A-B-C-D-1", group.getMetricIdentifier(metricName));
			// ignore all next filters for scope -  because scopeString cached with only first filter
			assertEquals("A-B-C-D-1", group.getMetricIdentifier(metricName, staticCharacterFilter));
			assertEquals("A-B-C-D-1", group.getMetricIdentifier(metricName, this));
			assertEquals("A-B-C-D-4", group.getMetricIdentifier(metricName, new CharacterFilter() {
				@Override
				public String filterCharacters(String input) {
					return input.replace("B", "RR").replace("1", "4");
				}
			}));
			countSuccessChecks++;
		}
	}
	public static class TestReporter2 extends TestReporter1 {
		@Override
		public String filterCharacters(String input) {
			return input.replace("B", "RR");
		}
		@Override
		public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
			assertEquals("A!RR!C!D!1", group.getMetricIdentifier(metricName, this));
			// ignore all next filters -  because scopeString cached with only first filter
			assertEquals("A!RR!C!D!1", group.getMetricIdentifier(metricName));
			assertEquals("A!RR!C!D!1", group.getMetricIdentifier(metricName, staticCharacterFilter));
			assertEquals("A!RR!C!D!3", group.getMetricIdentifier(metricName, new CharacterFilter() {
				@Override
				public String filterCharacters(String input) {
					return input.replace("A", "RR").replace("1", "3");
				}
			}));
			countSuccessChecks++;
		}
	}
	static class MetricRegistryTest extends MetricRegistry {
		protected static int countSuccessChecks = 0;
		public MetricRegistryTest(MetricRegistryConfiguration config) {
			super(config);
		}

		void checkMethod(Metric metric, String metricName, AbstractMetricGroup group) {
			try {
				// this filters will be use always because use incorrect of reporterIndex
				assertEquals("A.B.RR.D.1", group.getMetricIdentifier(metricName, staticCharacterFilter));
				assertEquals("A.B.RR.D.1", group.getMetricIdentifier(metricName, staticCharacterFilter, getReporters().size() + 2));

				if (getReporters() != null) {
					for (int i = 0; i < getReporters().size(); i++) {
						MetricReporter reporter = getReporters().get(i);
						if (reporter != null) {
							if (reporter instanceof CharacterFilter) {
								if (reporter instanceof TestReporter2) {
									assertEquals("A.RR.C.D.1", group.getMetricIdentifier(metricName, (CharacterFilter) reporter));
									assertEquals("A.RR.C.D.1", group.getMetricIdentifier(metricName, (CharacterFilter) reporter, getReporters().size() + 2));
									assertEquals("A.B.C.D.1", group.getMetricIdentifier(metricName, null, getReporters().size() + 2));
									TestReporter1.countSuccessChecks++;
								} else if (reporter instanceof TestReporter1) {
									assertEquals("RR.B.C.D.1", group.getMetricIdentifier(metricName, (CharacterFilter) reporter));
									assertEquals("RR.B.C.D.1", group.getMetricIdentifier(metricName, (CharacterFilter) reporter, getReporters().size() + 2));
									assertEquals("A.B.C.D.1", group.getMetricIdentifier(metricName, null, getReporters().size() + 2));
									TestReporter1.countSuccessChecks++;
								} else {
									fail("Unknown reporter class: " + reporter.getClass().getSimpleName());
								}
							}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				fail(e.toString());
			}
		}
		@Override
		public void register(Metric metric, String metricName, AbstractMetricGroup group) {
			checkMethod(metric, metricName, group);
			super.register(metric, metricName, group);
			countSuccessChecks++;
		}
	}
}
