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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.le.util.CodingUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * Builder used to create the JSON to push metrics to KairosDB.
 */
public class MetricBuilder {

    private List<Metric> metrics = new ArrayList<Metric>();
    private transient Gson mapper;

    private MetricBuilder() {
        GsonBuilder builder = new GsonBuilder();
        mapper = builder.create();
    }

    /**
     * Returns a new metric builder.
     *
     * @return metric builder
     */
    public static MetricBuilder getInstance() {
        return new MetricBuilder();
    }

    public Metric setMetric(Metric metric) {
        metrics.add(metric);
        return metric;
    }

    /**
     * Adds a metric to the builder.
     *
     * @param metricName metric name
     * @return the new metric
     */
    public Metric addMetric(String metricName) {
        Metric metric = new Metric(metricName);
        metrics.add(metric);
        return metric;
    }

    /**
     * Adds a metric to the builder.
     *
     * @param
     * @return the new metric
     */
    public Metric addMetric(Metric metric) {
        metrics.add(metric);
        return metric;
    }

    /**
     * Returns a list of metrics added to the builder.
     *
     * @return list of metrics
     */
    public List<Metric> getMetrics() {
        return metrics;
    }

    /**
     * Returns the JSON string built by the builder. This is the JSON that can
     * be used by the client add metrics.
     *
     * @return JSON
     * @throws IOException if metrics cannot be converted to JSON
     */
    public String build() throws IOException {
        for (Metric metric : metrics) {
            checkState(metric.getTags().size() > 0, metric.getMetric() + " must contain at least one tag.");
        }

        return CodingUtil.GBK2Unicode(mapper.toJson(metrics).replace("？", "").replace("。", "").replace("！", "")
                .replace("，", "").replace(" ", "").replace("?", "").replace("!", ""));
        // return mapper.toJson(metrics);
    }

    public void clear() {
        metrics.clear();
    }
}
