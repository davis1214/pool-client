/*
 * Copyright 2015-2016 Dark Phoenixs (Open-Source Organization).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.le.pool.opentsdb;

import com.le.client.opentsdb.OpentsdbClient;
import com.le.pool.tool.ConnectionException;
import com.le.pool.tool.ConnectionFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

class OpentsdbConnectionFactory implements ConnectionFactory<OpentsdbClient> {

    private static AtomicInteger counter = new AtomicInteger(0);

    /**
     * opentsdbUrl
     */
    private final String opentsdbUrl;

    /**
     * <p>Title: OpentsdbConnectionFactory</p>
     * <p>Description: 构造方法</p>
     *
     * @param properties
     */
    public OpentsdbConnectionFactory(final Properties properties) {
        this.opentsdbUrl = properties.getProperty(OpentsdbConfig.DEFAULT_JDBC_URL);
        if (opentsdbUrl == null)
            throw new ConnectionException("[" + OpentsdbConfig.DEFAULT_JDBC_URL + "] is required !");
    }

    /**
     * <p>Title: OpentsdbConnectionFactory</p>
     * <p>Description: 构造方法</p>
     *
     * @param opentsdbUrl opentsdb连接地址
     */
    public OpentsdbConnectionFactory(final String opentsdbUrl) {
        this.opentsdbUrl = opentsdbUrl;

        if (this.opentsdbUrl == null)
            throw new ConnectionException("[" + OpentsdbConfig.DEFAULT_JDBC_URL + "] is required !");
    }


    @Override
    public PooledObject<OpentsdbClient> makeObject() throws Exception {
        OpentsdbClient connection = this.createConnection();
        return new DefaultPooledObject<OpentsdbClient>(connection);
    }

    @Override
    public void destroyObject(PooledObject<OpentsdbClient> p) throws Exception {
        OpentsdbClient connection = p.getObject();
        if (connection != null)
            connection.shutdown();
    }

    @Override
    public boolean validateObject(PooledObject<OpentsdbClient> p) {
        OpentsdbClient connection = p.getObject();
        return connection != null;
    }

    @Override
    public void activateObject(PooledObject<OpentsdbClient> p) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void passivateObject(PooledObject<OpentsdbClient> p) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public OpentsdbClient createConnection() throws Exception {

        if (this.opentsdbUrl.contains(",")) {

            String[] opentsdbUrls = this.opentsdbUrl.split(",");

            int take = counter.get() % opentsdbUrls.length;

            int count = 0;
            boolean shouldStop = false;
            String targetUrl = null;
            while (!shouldStop && count < opentsdbUrls.length) {
                targetUrl = opentsdbUrls[take];

                if (targetUrl != null && targetUrl.length() > 0) {
                    shouldStop = true;
                }
                count++;
            }

            counter.getAndIncrement();
            OpentsdbClient client = new OpentsdbClient(targetUrl);
            return client;
        }

        OpentsdbClient client = new OpentsdbClient(this.opentsdbUrl);
        return client;
    }

}
