package com.le.pool.es;

import com.le.client.es.ESSerializer;
import com.le.client.es.IndexNameBuilder;
import com.le.pool.elastic.search.ESConnectionPool;
import com.le.pool.tool.ConnectionPoolConfig;
import junit.framework.Assert;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


public class ESClientTest {

    private String S_TRACE_INDEX_TYPE = "log";
    private Client client = null;
    private BulkRequestBuilder bulkRequestBuilder = null;

    private IndexNameBuilder indexNameBuilder = null;

    private static ESSerializer serializer = null;
    private long lastPutTime = System.currentTimeMillis();

    private static final String DEFAULT_SERIALIZER_CLASS = "com.weidian.vtrace.elasticsearch.ESVshopRecordSerializer";
    private static final String DEFAULT_INDEX_NAME_BUILDER_CLASS = "com.weidian.vtrace.elasticsearch.TimeBasedIndexNameBuilder";

    private static final int BULK_RETRY_TIMES = 3;
    private static final int BULK_RETRY_WAIT_TIME = 5 * 1000;

    private Map<String, String> conf = new HashMap<>();
    private ESConnectionPool pool;

    @Before
    public void before() {

        ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig();

        String[] hostNames = new String[]{"localhost:9300"};
        String clusterName = "test";

        pool = new ESConnectionPool(connectionPoolConfig, hostNames, clusterName);

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

    public static final String vlog = "NOTICE: 2016-09-20 15:56:50.859 10.2.22.158 [Base.php:256 Base::printLog()] traceid[1469000001574698f11d0a02083d54ee,0] logid[147435821058703067] url[/wd/user/getCertStatus] req[{\"traceID\":\"router_633gu17xsqisr4tz3t1152\",\"scheme\":\"https\",\"param\":\"{\"userID\":\"903042\",\"wduss\":\"f494334aac5c2f30c7fd0516a20af9a20445514155238523d814f428b3984b7c\"}\",\"public\":\"{\"kdchainid\":\"032E92BC-EC...] public[kdchainid:032E92BC-EC4D-4347-9110-C78C62DD9722\"|\"appid:com.koudai.weishop\"|\"aguid:1474352056349_326838\"|\"imsi:\"|\"mid:iPhone\"|\"userID:903042\"|\"brand:Apple\"|\"channel:1000f\"|\"device_id:100332128\"|\"uuid:4C29B60A-7222-462D-BE22-C27C7F2E9D81\"|\"imei:\"|\"apiv:750\"|\"h:1334\"|\"appstatus:active\"|\"version:7.5.0\"|\"os:10.0.1\"|\"w:750\"|\"udid:\"|\"build:20160704150947\"|\"openudid:9532fc3851e9498e50f0e85fe28abdadb2d43794\"|\"mac:02:00:00:00:00:00\"|\"platform:iphone\"|\"proxy_timestamp:1474358210526\"|\"sessionid:ks_1_1474358209177_4322_1\"|\"network:3G\"|\"machineName:iPhone8,1\"|\"wmac:40:e3:d6:d5:90:92\"|\"wduss:f494334aac5c2f30c7fd0516a20af9a20445514155238523d814f428b3984b7c\"|\"wssid:koudai-office\"|\"idfa:8ACF839C-7A78-4582-94C9-E60073043D9D\"|\"referService:proxy-wd\"|\"x-forwarded-for:172.16.27.143] loginfo[localip:10.2.8.61\"|\"module:wd\"|\"mem_call_num:1\"|\"userID:903042\"|\"platform:iphone\"|\"errno:0\"|\"errmsg:\"|\"real_ip:172.16.27.143 ] time[ t_passport_0:10 db_con:0 total:15 mem_get:1 t_redis_getm:1 t_redis_con:0 t_redis_con_redis_wd:0 db_query:1 db_query_vshop_r:1 db_con_vshop_r:0 ]";

    @Test
    public void testExist() {
        // String indexName = "trace_vshop_log_test";
        String indexName = "test_index";
        IndicesExistsResponse response = client.admin().indices()
                .exists(new IndicesExistsRequest().indices(new String[]{indexName})).actionGet();

        System.err.println("indexName exists : " + response.isExists());
    }

    @Test
    public void getLastIndexName() {
        String indexName = "di_shop_doc*";
        int span = 0;

        GetIndexResponse actionGet = client.admin().indices()
                .getIndex(new GetIndexRequest().indices(new String[]{indexName})).actionGet();

        String[] indices = actionGet.getIndices();

        System.out.println("length :" + indices.length);

        if (indices == null || indices.length == 0) {
            Assert.assertFalse(true);
        }

        Arrays.sort(indices);

        System.out.println(indices[indices.length - 1 - span]);
        Assert.assertFalse(false);
    }

    @Test
    public void testIndex() {
        // String indexName = "trace_vshop_log_test";
        String indexName = "record_wditem_sold*";
        IndicesExistsResponse response = client.admin().indices()
                .exists(new IndicesExistsRequest().indices(new String[]{indexName})).actionGet();

        System.err.println("indexName exists : " + response.isExists());

        GetIndexResponse actionGet = client.admin().indices()
                .getIndex(new GetIndexRequest().indices(new String[]{indexName})).actionGet();

        String[] indices = actionGet.getIndices();

        System.out.println("sss->" + Arrays.toString(indices));

        Arrays.sort(indices);

        System.out.println("sss->" + Arrays.toString(indices));

        for (String string : indices) {
            System.out.println("sss->" + string);
        }

    }




}
