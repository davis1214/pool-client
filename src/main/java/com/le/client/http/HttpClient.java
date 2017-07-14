package com.le.client.http;

import com.le.client.opentsdb.builder.MetricBuilder;
import com.le.client.opentsdb.request.QueryBuilder;
import com.le.client.opentsdb.response.Response;
import com.le.client.opentsdb.response.SimpleHttpResponse;

import java.io.IOException;

public interface HttpClient extends Client {

	public Response pushMetrics(MetricBuilder builder,
								ExpectResponse exceptResponse) throws IOException;

	public SimpleHttpResponse pushQueries(QueryBuilder builder,
										  ExpectResponse exceptResponse) throws IOException;

    public void shutdown();
}