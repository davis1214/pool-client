/*
 * Copyright 2013 Proofpoint Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.le.client.opentsdb.builder;

import com.google.gson.annotations.SerializedName;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.le.client.opentsdb.tool.Preconditions.checkNotNullOrEmpty;

/**
 * A metric contains measurements or data points. Each data point has a time
 * stamp of when the measurement occurred and a value that is either a long or
 * double and optionally contains tags. Tags are labels that can be added to
 * better identify the metric. For example, if the measurement was done on
 * server1 then you might add a tag named "host" with a value of "server1". Note
 * that a metric must have at least one tag.
 */
public class Metric {

//	@SerializedName("metric")
//	@Expose
//	private String metric;
//
//	@Expose
//	private long timestamp;
//
//	@Expose
//	private Object value;
//
//	private String metricKey;
//
//	@Expose
//	private Map<String, String> tags = new HashMap<String, String>();
//
//	protected Metric(String name) {
//		this.metric = checkNotNullOrEmpty(name);
//	}
//
//	public Metric(String name, long timestamp, Object value, Map<String, String> tags) {
//		this.metric = name;
//		this.timestamp = timestamp;
//		this.value = value;
//		this.tags = tags;
//
//		StringBuffer buffer = new StringBuffer();
//		buffer.append(this.metric).append(this.timestamp);
//
//		for (String key : tags.keySet()) {
//			buffer.append(key);
//		}
//
//		this.metricKey = buffer.toString();
//	}
//
//	public String getKey() {
//		return metricKey;
//	}
//
//	public void setMetricKey(String metricKey) {
//		this.metricKey = metricKey;
//	}
//
//	/**
//	 * Adds a tag to the data point.
//	 *
//	 * @param name
//	 *            tag identifier
//	 * @param value
//	 *            tag value
//	 * @return the metric the tag was added to
//	 */
//	public Metric addTag(String name, String value) {
//		checkNotNullOrEmpty(name);
//		checkNotNullOrEmpty(value);
//		tags.put(name, value);
//
//		return this;
//	}
//
//	/**
//	 * Adds tags to the data point.
//	 *
//	 * @param tags
//	 *            map of tags
//	 * @return the metric the tags were added to
//	 */
//	public Metric addTags(Map<String, String> tags) {
//		checkNotNull(tags);
//		this.tags.putAll(tags);
//
//		return this;
//	}
//
//	/**
//	 * set the data point for the metric.
//	 *
//	 * @param timestamp
//	 *            when the measurement occurred
//	 * @param value
//	 *            the measurement value
//	 * @return the metric
//	 */
//	protected Metric innerAddDataPoint(long timestamp, Object value) {
//		checkArgument(timestamp > 0);
//		this.timestamp = timestamp;
//		this.value = checkNotNull(value);
//
//		return this;
//	}
//
//	/**
//	 * Adds the data point to the metric with a timestamp of now.
//	 *
//	 * @param value
//	 *            the measurement value
//	 * @return the metric
//	 */
//	public Metric setDataPoint(long value) {
//		return innerAddDataPoint(System.currentTimeMillis(), value);
//	}
//
//	public Metric setDataPoint(long timestamp, long value) {
//		return innerAddDataPoint(timestamp, value);
//	}
//
//	/**
//	 * Adds the data point to the metric.
//	 *
//	 * @param timestamp
//	 *            when the measurement occurred
//	 * @param value
//	 *            the measurement value
//	 * @return the metric
//	 */
//	public Metric setDataPoint(long timestamp, double value) {
//		return innerAddDataPoint(timestamp, value);
//	}
//
//	/**
//	 * Adds the data point to the metric with a timestamp of now.
//	 *
//	 * @param value
//	 *            the measurement value
//	 * @return the metric
//	 */
//	public Metric setDataPoint(double value) {
//		return innerAddDataPoint(System.currentTimeMillis(), value);
//	}
//
//	/**
//	 * Time when the data point was measured.
//	 *
//	 * @return time when the data point was measured
//	 */
//	public long getTimestamp() {
//		return timestamp;
//	}
//
//	public Object getValue() {
//		return value;
//	}
//
//	// public String stringValue() throws DataFormatException {
//	// return value.toString();
//	// }
//	//
//	public long getLongValue() {
//		try {
//			return ((Number) value).longValue();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return 0l;
//	}
//
//	public double getDoubleValue() {
//		try {
//			return ((Number) value).doubleValue();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return 0l;
//	}
//
//	// public double doubleValue() throws DataFormatException {
//	// try {
//	// return ((Number) value).doubleValue();
//	// } catch (Exception e) {
//	// throw new DataFormatException("Value is not a double");
//	// }
//	// }
//
//	public boolean isDoubleValue() {
//		return !(((Number) value).doubleValue() == Math.floor(((Number) value).doubleValue()));
//	}
//
//	public boolean isIntegerValue() {
//		return ((Number) value).doubleValue() == Math.floor(((Number) value).doubleValue());
//	}
//
//	/**
//	 * Returns the metric name.
//	 *
//	 * @return metric name
//	 */
//	public String getMetric() {
//		return metric;
//	}
//
//	/**
//	 * Returns the tags associated with the data point.
//	 *
//	 * @return tag for the data point
//	 */
//	public Map<String, String> getTags() {
//		return Collections.unmodifiableMap(tags);
//	}
//
//	/**
//	 * Set the value
//	 */
//	public void setValue(Object value) {
//		this.value = value;
//	}
//
//	public Boolean equal(Metric metric) {
//		if (metric.getMetric().equals(this.getMetric()) && metric.getTimestamp() == this.getTimestamp()
//				&& metric.getTags().equals(this.getTags())) {
//			return true;
//		} else {
//			return false;
//		}
//	}
//
//	public static void main(String[] args) {
//		Map<String, String> tags1 = new HashMap<>();
//		tags1.put("a", "a");
//		tags1.put("b", "b");
//
//		Map<String, String> tags2 = new HashMap<>();
//		tags2.put("b", "b");
//		tags2.put("a", "a");
//		Metric metric1 = new Metric("m1", 10000l, 10, tags1);
//		Metric metric2 = new Metric("m1", 10000l, 11, tags2);
//		if (metric1.equal(metric2)) {
//			System.out.println("YES");
//		}
//	}
@SerializedName("metric")
private String metric;

	private long timestamp;

	private Object value;

	private Map<String, String> tags = new HashMap<String, String>();

	protected Metric(String name) {
		this.metric = checkNotNullOrEmpty(name);
	}

	public Metric(String name, long timestamp, Object value, Map<String, String> tags) {
		this.metric = name;
		this.timestamp = timestamp;
		this.value = value;
		this.tags = tags;
	}

	/**
	 * Adds a tag to the data point.
	 *
	 * @param name
	 *            tag identifier
	 * @param value
	 *            tag value
	 * @return the metric the tag was added to
	 */
	public Metric addTag(String name, String value) {
		checkNotNullOrEmpty(name);
		checkNotNullOrEmpty(value);
		tags.put(name, value);

		return this;
	}

	/**
	 * Adds tags to the data point.
	 *
	 * @param tags
	 *            map of tags
	 * @return the metric the tags were added to
	 */
	public Metric addTags(Map<String, String> tags) {
		checkNotNull(tags);
		this.tags.putAll(tags);

		return this;
	}

	/**
	 * set the data point for the metric.
	 *
	 * @param timestamp
	 *            when the measurement occurred
	 * @param value
	 *            the measurement value
	 * @return the metric
	 */
	protected Metric innerAddDataPoint(long timestamp, Object value) {
		checkArgument(timestamp > 0);
		this.timestamp = timestamp;
		this.value = checkNotNull(value);

		return this;
	}

	/**
	 * Adds the data point to the metric with a timestamp of now.
	 *
	 * @param value
	 *            the measurement value
	 * @return the metric
	 */
	public Metric setDataPoint(long value) {
		return innerAddDataPoint(System.currentTimeMillis(), value);
	}

	public Metric setDataPoint(long timestamp, long value) {
		return innerAddDataPoint(timestamp, value);
	}

	/**
	 * Adds the data point to the metric.
	 *
	 * @param timestamp
	 *            when the measurement occurred
	 * @param value
	 *            the measurement value
	 * @return the metric
	 */
	public Metric setDataPoint(long timestamp, double value) {
		return innerAddDataPoint(timestamp, value);
	}

	/**
	 * Adds the data point to the metric with a timestamp of now.
	 *
	 * @param value
	 *            the measurement value
	 * @return the metric
	 */
	public Metric setDataPoint(double value) {
		return innerAddDataPoint(System.currentTimeMillis(), value);
	}

	/**
	 * Time when the data point was measured.
	 *
	 * @return time when the data point was measured
	 */
	public long getTimestamp() {
		return timestamp;
	}

	public Object getValue() {
		return value;
	}

//	public String stringValue() throws DataFormatException {
//		return value.toString();
//	}
//
//	public long longValue() throws DataFormatException {
//		try {
//			return ((Number) value).longValue();
//		} catch (Exception e) {
//			throw new DataFormatException("Value is not a long");
//		}
//	}

	// public double doubleValue() throws DataFormatException {
	// try {
	// return ((Number) value).doubleValue();
	// } catch (Exception e) {
	// throw new DataFormatException("Value is not a double");
	// }
	// }

	public boolean isDoubleValue() {
		return !(((Number) value).doubleValue() == Math.floor(((Number) value).doubleValue()));
	}

	public boolean isIntegerValue() {
		return ((Number) value).doubleValue() == Math.floor(((Number) value).doubleValue());
	}

	/**
	 * Returns the metric name.
	 *
	 * @return metric name
	 */
	public String getMetric() {
		return metric;
	}

	/**
	 * Returns the tags associated with the data point.
	 *
	 * @return tag for the data point
	 */
	public Map<String, String> getTags() {
		return Collections.unmodifiableMap(tags);
	}
	/**
	 * Set the value
	 */
	public void setValue(Object value){
		this.value = value;
	}

	public Boolean equal(Metric metric) {
		if (metric.getMetric().equals(this.getMetric()) && metric.getTimestamp() == this.getTimestamp()
				&& metric.getTags().equals(this.getTags())) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "metric=" + metric + ", timestamp=" + String.valueOf(timestamp) + ", value=" + String.valueOf(value) + ", tags=" + tags;
	}
}
