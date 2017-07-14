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
import com.le.pool.tool.ConnectionPool;
import com.le.pool.tool.ConnectionPoolBase;
import com.le.pool.tool.ConnectionPoolConfig;

import java.util.Properties;

public class OpentsdbConnectionPool extends ConnectionPoolBase<OpentsdbClient> implements ConnectionPool<OpentsdbClient> {


    /**
     * <p>Title: OpentsdbConnectionPool</p>
     * <p>Description: 默认构造方法</p>
     */
    public OpentsdbConnectionPool() {

        this(OpentsdbConfig.DEFAULT_JDBC_URL);
    }

    /**
     * <p>Title: OpentsdbConnectionPool</p>
     * <p>Description: 构造方法</p>
     *
     * @param properties JDBC参数
     */
    public OpentsdbConnectionPool(final Properties properties) {

        this(new ConnectionPoolConfig(), properties);
    }

    /**
     * <p>Title: OpentsdbConnectionPool</p>
     * <p>Description: 构造方法</p>
     *
     * @param opentsdbUrl opentsdbUrl
     */
    public OpentsdbConnectionPool(final String opentsdbUrl) {

        this(new ConnectionPoolConfig(), opentsdbUrl);
    }

    /**
     * <p>Title: OpentsdbConnectionPool</p>
     * <p>Description: 构造方法</p>
     *
     * @param poolConfig 池配置
     * @param properties JDBC参数
     */
    public OpentsdbConnectionPool(final ConnectionPoolConfig poolConfig, final Properties properties) {

        super(poolConfig, new OpentsdbConnectionFactory(properties));
    }

    /**
     * <p>Title: OpentsdbConnectionPool</p>
     * <p>Description: 构造方法</p>
     *
     * @param poolConfig  池配置
     * @param opentsdbUrl opentsdb url
     */
    public OpentsdbConnectionPool(final ConnectionPoolConfig poolConfig, final String opentsdbUrl) {

        super(poolConfig, new OpentsdbConnectionFactory(opentsdbUrl));
    }

    @Override
    public OpentsdbClient getConnection() {

        return super.getResource();
    }

    @Override
    public void returnConnection(OpentsdbClient conn) {

        super.returnResource(conn);
    }

    @Override
    public void invalidateConnection(OpentsdbClient conn) {
        super.invalidateResource(conn);
    }

}
