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
package com.le.pool.elastic.search;

import com.le.pool.tool.ConnectionPool;
import com.le.pool.tool.ConnectionPoolBase;
import com.le.pool.tool.ConnectionPoolConfig;
import org.elasticsearch.client.transport.TransportClient;

public class ESConnectionPool extends ConnectionPoolBase<TransportClient> implements ConnectionPool<TransportClient> {


    /**
     * <p>Title: ESConnectionPool</p>
     * <p>Description: 构造方法</p>
     *
     * @param poolConfig  池配置
     * @param hostNames   hostNames列表
     * @param clusterName 集群名称
     */
    public ESConnectionPool(final ConnectionPoolConfig poolConfig, final String[] hostNames, final String clusterName) {
        super(poolConfig, new ESConnectionFactory(hostNames, clusterName));
    }

    @Override
    public TransportClient getConnection() {
        return super.getResource();
    }

    @Override
    public void returnConnection(TransportClient conn) {
        super.returnResource(conn);
    }

    @Override
    public void invalidateConnection(TransportClient conn) {
        super.invalidateResource(conn);
    }
}
