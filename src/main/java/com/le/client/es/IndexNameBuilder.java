package com.le.client.es;

import java.util.Map;

public interface IndexNameBuilder {
    public String getIndexName(Object object);

    void configure(Map conf);
}
