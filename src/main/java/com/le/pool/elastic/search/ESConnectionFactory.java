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

import com.le.pool.tool.ConnectionFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

class ESConnectionFactory implements ConnectionFactory<TransportClient> {


    private String[] hostNames;
    private String clusterName;
    private int DEFAULT_PORT = 9300;

    /**
     * <p>Title: ESConnectionFactory</p>
     * <p>Description: 构造方法</p>
     *
     * @param hostNames 配置
     */
    public ESConnectionFactory(final String[] hostNames, final String clusterName) {

        this.hostNames = hostNames;
        this.clusterName = clusterName;
    }

    @Override
    public PooledObject<TransportClient> makeObject() throws Exception {
        TransportClient transportClient = this.createConnection();
        return new DefaultPooledObject<TransportClient>(transportClient);
    }

    @Override
    public void destroyObject(PooledObject<TransportClient> p)
            throws Exception {
        TransportClient transportClient = p.getObject();

        if (null != transportClient)
            transportClient.close();
    }

    @Override
    public boolean validateObject(PooledObject<TransportClient> p) {
        TransportClient transportClient = p.getObject();
        return (null != transportClient);
    }

    @Override
    public void activateObject(PooledObject<TransportClient> p)
            throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void passivateObject(PooledObject<TransportClient> p)
            throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public TransportClient createConnection() throws Exception {
        InetSocketTransportAddress[] serverAddresses = new InetSocketTransportAddress[hostNames.length];
        for (int i = 0; i < hostNames.length; i++) {
            String[] hostPort = hostNames[i].trim().split(":");
            String host = hostPort[0].trim();
            int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1].trim()) : DEFAULT_PORT;
            serverAddresses[i] = new InetSocketTransportAddress(host, port);
        }

        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName)
                .put("client.transport.sniff", false) // 嗅探集群的其它节点
                .build();

        TransportClient transportClient = new TransportClient(settings);
        for (InetSocketTransportAddress host : serverAddresses) {
            transportClient.addTransportAddress(host);
        }
        return transportClient;
    }
}
