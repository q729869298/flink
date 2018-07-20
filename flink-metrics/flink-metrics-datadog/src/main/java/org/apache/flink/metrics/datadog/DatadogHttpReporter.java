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

package org.apache.flink.metrics.datadog;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metric Reporter for Datadog.
 *
 * <p>Variables in metrics scope will be sent to Datadog as tags.
 */
public class DatadogHttpReporter implements MetricReporter, Scheduled {
	private static final Logger LOGGER = LoggerFactory.getLogger(DatadogHttpReporter.class);
	private static final String HOST_VARIABLE = "<host>";

	// Both Flink's Gauge and Meter values are taken as gauge in Datadog
	private final Map<Gauge, DGauge> gauges = new ConcurrentHashMap<>();
	private final Map<Counter, DCounter> counters = new ConcurrentHashMap<>();
	private final Map<Meter, DMeter> meters = new ConcurrentHashMap<>();

	private DatadogHttpClient client;
	private List<String> configTags;
	private String overrideHostname;
	private boolean removeValueGroups;

	public static final String API_KEY = "apikey";
	public static final String OVERRIDE_HOSTNAME = "override-hostname";
	public static final String TAGS = "tags";
	public static final String REMOVE_VALUE_GROUPS = "remove-value-groups";

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
		final String name = buildMetricName(group, metricName);
		LOGGER.debug("new metric {}, scopes: {}", name, Arrays.toString(group.getScopeComponents()));

		List<String> tags = new ArrayList<>(configTags);
		tags.addAll(getTagsFromMetricGroup(group));
		String host = getHostName(group);

		if (metric instanceof Counter) {
			Counter c = (Counter) metric;
			counters.put(c, new DCounter(c, name, host, tags));
		} else if (metric instanceof Gauge) {
			Gauge g = (Gauge) metric;
			gauges.put(g, new DGauge(g, name, host, tags));
		} else if (metric instanceof Meter) {
			Meter m = (Meter) metric;
			// Only consider rate
			meters.put(m, new DMeter(m, name, host, tags));
		} else if (metric instanceof Histogram) {
			LOGGER.warn("Cannot add {} because Datadog HTTP API doesn't support Histogram", metricName);
		} else {
			LOGGER.warn("Cannot add unknown metric type {}. This indicates that the reporter " +
				"does not support this metric type.", metric.getClass().getName());
		}
	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
		if (metric instanceof Counter) {
			counters.remove(metric);
		} else if (metric instanceof Gauge) {
			gauges.remove(metric);
		} else if (metric instanceof Meter) {
			meters.remove(metric);
		} else if (metric instanceof Histogram) {
			// No Histogram is registered
		} else {
			LOGGER.warn("Cannot remove unknown metric type {}. This indicates that the reporter " +
				"does not support this metric type.", metric.getClass().getName());
		}
	}

	@Override
	public void open(MetricConfig config) {
		client = buildClient(config);
		if (config.getBoolean(OVERRIDE_HOSTNAME, false)) {
			overrideHostname = System.getenv("HOSTNAME");
		}
		removeValueGroups = config.getBoolean(REMOVE_VALUE_GROUPS, false);
		LOGGER.info("Configured DatadogHttpReporter");

		configTags = getTagsFromConfig(config.getString(TAGS, ""));
	}

	@Override
	public void close() {
		client.close();
		LOGGER.info("Shut down DatadogHttpReporter");
	}

	@Override
	public void report() {
		DatadogHttpRequest request = new DatadogHttpRequest();

		List<Gauge> gaugesToRemove = new ArrayList<>();
		for (Map.Entry<Gauge, DGauge> entry : gauges.entrySet()) {
			DGauge g = entry.getValue();
			try {
				// Will throw exception if the Gauge is not of Number type
				// Flink uses Gauge to store many types other than Number
				g.getMetricValue();
				request.addGauge(g);
			} catch (ClassCastException e) {
				LOGGER.info("The metric {} will not be reported because only number types are supported by this reporter.", g.getMetric());
				gaugesToRemove.add(entry.getKey());
			} catch (Exception e) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("The metric {} will not be reported because it threw an exception.", g.getMetric(), e);
				} else {
					LOGGER.info("The metric {} will not be reported because it threw an exception.", g.getMetric());
				}
				gaugesToRemove.add(entry.getKey());
			}
		}
		gaugesToRemove.forEach(gauges::remove);

		for (DCounter c : counters.values()) {
			request.addCounter(c);
		}

		for (DMeter m : meters.values()) {
			request.addMeter(m);
		}

		try {
			client.send(request);
		} catch (SocketTimeoutException e) {
			LOGGER.warn("Failed reporting metrics to Datadog because of socket timeout.", e.getMessage());
		} catch (Exception e) {
			LOGGER.warn("Failed reporting metrics to Datadog.", e);
		}
	}

	/**
	 * Get config tags from config 'metrics.reporter.dghttp.tags'.
	 */
	private List<String> getTagsFromConfig(String str) {
		return Arrays.asList(str.split(","));
	}

	/**
	 * Get tags from MetricGroup#getAllVariables(), excluding 'host'.
	 */
	private List<String> getTagsFromMetricGroup(MetricGroup metricGroup) {
		List<String> tags = new ArrayList<>();

		for (Map.Entry<String, String> entry: metricGroup.getAllVariables().entrySet()) {
			if (!entry.getKey().equals(HOST_VARIABLE)) {
				tags.add(getVariableName(entry.getKey()) + ":" + entry.getValue());
			}
		}

		return tags;
	}

	/**
	 * Returns either the returned hostname, or uses the overriden hostname
	 * from the `HOSTNAME` env var.
	 * @param metricGroup the metric group hostname
	 * @return a hostname
	 */
	String getHostName(MetricGroup metricGroup) {
		if (overrideHostname != null) {
			return overrideHostname;
		} else {
			return metricGroup.getAllVariables().get(HOST_VARIABLE);
		}
	}

	/**
	 * This attempts to build a metric name devoid of any "value" type
	 * metricGroups. Currently, the metricGroup API does not give a nice
	 * way of traversing the scopes with any information about whether those
	 * metrics groups or keys are values.
	 *
	 * <p>For a system like datadog, it is important that we strip any elements
	 * that are values from the metricName, as it makes it hard
	 * to group by in datadog. Instead, those values are turned into
	 * tags via the "variables"
	 *
	 * <p>This works by looking through the scopes and assumes that for any
	 * element of scopes, if the previous scope is a key, then the current
	 * element will be the "value" in the variables hash with the previous element
	 * as the key. Other other elements are added
	 * @param group the metric group for the new metric
	 * @return the metric name
	 */
	String buildMetricName(MetricGroup group, String name) {
		if (!removeValueGroups) {
			return group.getMetricIdentifier(name);
		} else {
			return stripValueGroups(group, name);
		}
	}

	DatadogHttpClient buildClient(MetricConfig config) {
		return new DatadogHttpClient(config.getString(API_KEY, null));
	}

	private String stripValueGroups(MetricGroup group, String name) {
		String[] components = group.getScopeComponents();
		Map<String, String>	 vars = group.getAllVariables();
		ArrayList<String> parts = new ArrayList<>();
		for (int i = 0; i < components.length; i++) {
			String component = components[i];
			if (i > 0 && vars.containsKey(wrapVariableName(components[i - 1]))) {
				if (!vars.get(wrapVariableName(components[i - 1])).equals(component)) {
					parts.add(component);
				}
			} else {
				parts.add(component);
			}
		}
		parts.add(name);

		return String.join(".", parts);
	}

	/**
	 * Removes leading and trailing angle brackets.
	 */
	private String getVariableName(String str) {
		return str.substring(1, str.length() - 1);
	}

	/**
	 * Add angle brackets to be proper variable.
	 */
	private String wrapVariableName(String str) {
		return "<" + str + ">";
	}

	/**
	 * Compact metrics in batch, serialize them, and send to Datadog via HTTP.
	 */
	static class DatadogHttpRequest {
		private final DSeries series;

		public DatadogHttpRequest() {
			series = new DSeries();
		}

		public void addGauge(DGauge gauge) {
			series.addMetric(gauge);
		}

		public void addCounter(DCounter counter) {
			series.addMetric(counter);
		}

		public void addMeter(DMeter meter) {
			series.addMetric(meter);
		}

		public DSeries getSeries() {
			return series;
		}
	}
}
