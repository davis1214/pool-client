package com.le.client.es;

import org.elasticsearch.common.io.BytesStream;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.charset.Charset;

public interface ESSerializer {
    public static final Charset charset = Charset.defaultCharset();

    /**
     * Return an {@link BytesStream} made up of the serialized Object
     * @param log
     *          The Object to serialize
     * @return A {@link BytesStream} used to write to ElasticSearch
     * @throws Exception
     *           If an error occurs during serialization
     */
    abstract XContentBuilder getContentBuilder(Object log) throws Exception;
}
