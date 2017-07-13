package com.le.pool.es;

import com.le.client.es.IndexNameBuilder;
import org.apache.commons.lang.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

public class TimeBasedIndexNameBuilder implements IndexNameBuilder {
    public String indexName;
    private FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd", TimeZone.getTimeZone("GMT+8"));


    public static final Logger logger = LoggerFactory.getLogger(TimeBasedIndexNameBuilder.class);

    public String getIndexName(Object object) {
        return new StringBuilder(indexName).append('-')
                .append(fastDateFormat.format(new Date())).toString();
    }

    public void configure(Map conf) {
        indexName = (String) conf.get("es.index.name");
    }
}
