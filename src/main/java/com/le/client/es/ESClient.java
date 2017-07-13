package com.le.client.es;

import com.le.pool.elastic.search.ESConnectionPool;
import com.le.pool.tool.ConnectionPoolConfig;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


public class ESClient {

    private String DEFAULT_INDEX_TYPE = "log";
    private String DEFAULT_ES_CLUSTER_HOST = "localhost:9300";
    private String DEFAULT_ES_CLUSTER_NAME = "localhost:9300";
    private Client client = null;
    private BulkRequestBuilder bulkRequestBuilder = null;

    private IndexNameBuilder indexNameBuilder = null;

    private static ESSerializer serializer = null;
    private long lastPutTime = System.currentTimeMillis();

    private static final String DEFAULT_SERIALIZER_CLASS = "com.le.pool.es.ESLogReflectSerializer";
    private static final String DEFAULT_INDEX_NAME_BUILDER_CLASS = "com.le.pool.es.TimeBasedIndexNameBuilder";

    private static final int BULK_RETRY_TIMES = 3;
    private static final int BULK_RETRY_WAIT_TIME = 5 * 1000;
    private static final int ES_STORE_BATCH_SIZE = 100;


    private Map<String, String> conf = new HashMap<>();
    private ESConnectionPool pool;

    public void prepare() {
        ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig();
        String[] hostNames = new String[]{DEFAULT_ES_CLUSTER_HOST};
        pool = new ESConnectionPool(connectionPoolConfig, hostNames, DEFAULT_ES_CLUSTER_NAME);

        client = pool.getConnection();
        bulkRequestBuilder = client.prepareBulk();

        try {
            Class clazz = Class.forName(DEFAULT_SERIALIZER_CLASS);
            serializer = (ESSerializer) clazz.newInstance();
            clazz = Class.forName(DEFAULT_INDEX_NAME_BUILDER_CLASS);
            indexNameBuilder = (IndexNameBuilder) clazz.newInstance();

            conf.put("op.running.model", "online");
            conf.put("op.rs.product", "busi_monitor");
            conf.put("es.index.name", "es_index_test_name");

            indexNameBuilder.configure(conf);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 判断索引是否存在
     *
     * @param indexName
     * @return
     */
    public boolean isIndexExist(String indexName) {
        IndicesExistsResponse response = client.admin().indices()
                .exists(new IndicesExistsRequest().indices(new String[]{indexName})).actionGet();
        return response.isExists();
    }

    /**
     * 获取名称相似的连续索引 取最大的
     *
     * @param indexName "index_name_seq*"
     */
    public String getLastIndexName(String indexName) {

        int span = 0;

        GetIndexResponse actionGet = client.admin().indices()
                .getIndex(new GetIndexRequest().indices(new String[]{indexName})).actionGet();

        String[] indices = actionGet.getIndices();

        System.out.println("length :" + indices.length);

        if (indices == null || indices.length == 0) {
            return null;
        }

        Arrays.sort(indices);

        return indices[indices.length - 1 - span];
    }

    public void write(String text) {
        try {
            XContentBuilder builder = serializer.getContentBuilder(text);

            if (builder == null) {
                return;
            }

            String indexname = indexNameBuilder.getIndexName(null);

            IndexRequestBuilder indexRequestBuilder = client.prepareIndex(indexname, DEFAULT_INDEX_TYPE)
                    .setSource(builder.bytes());
            bulkRequestBuilder.add(indexRequestBuilder);

            if (bulkRequestBuilder.numberOfActions() >=
                    ES_STORE_BATCH_SIZE) {
                bulkCommit();
                lastPutTime = System.currentTimeMillis();
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public Map<String, Object> read(String indexes, String searchResultSize, String searchValue) {
        Map<String, Object> result = new HashMap<>();

        try {
            Client client = pool.getConnection();
            SearchRequestBuilder builder = client
                    .prepareSearch(indexes)
                    // 搜索productindex,prepareSearch(String...
                    // indices)注意该方法的参数,可以搜索多个索引
                    .setTypes(DEFAULT_INDEX_TYPE).setSearchType(SearchType.DEFAULT).setFrom(0)
                    .setSize(Integer.valueOf(searchResultSize));

            QueryBuilder qb = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("seller_id", searchValue));

            builder.setQuery(qb);

            SearchResponse responsesearch = builder.execute().actionGet();

            long costTime = responsesearch.getTookInMillis();
            long resNum = responsesearch.getHits().getTotalHits();

            if (responsesearch.getHits().totalHits() == 0) {
                return result;
            }

            SearchHit[] hits = responsesearch.getHits().getHits();


            for (SearchHit searchHit : hits) {
                Map<String, Object> source = searchHit.getSource();

                for (Entry<String, Object> entry : source.entrySet()) {
                    result.put(entry.getKey(), entry.getValue());
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    public void bulkCommit() {
        try {
            BulkResponse bulkResponse = null;
            for (int i = 0; i < BULK_RETRY_TIMES; i++) {
                bulkResponse = bulkRequestBuilder.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    Thread.sleep(BULK_RETRY_WAIT_TIME);
                    continue;
                } else {
                    break;
                }
            }
            if (bulkResponse.hasFailures()) {
                String error = bulkResponse.buildFailureMessage();

                if (error.contains("UnavailableShardsException")) {
                    client = pool.getConnection();
                }
                System.out.println("***ES bulk error:" + error);
            } else {
                bulkRequestBuilder = client.prepareBulk();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
