package com.le.pool.redis;

import com.le.pool.tool.ConnectionPoolConfig;
import com.le.pool.tool.ConnectionPool;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class RedisClusterConnPool implements ConnectionPool<JedisCluster> {

    private final JedisCluster jedisCluster;

    /**
     * Instantiates a new Redis cluster conn pool.
     *
     * @param clusterNodes the jedis cluster nodes
     */
    public RedisClusterConnPool(final Set<HostAndPort> clusterNodes) {

        this(clusterNodes, RedisConfig.DEFAULT_TIMEOUT);
    }

    /**
     * Instantiates a new Redis cluster conn pool.
     *
     * @param clusterNodes the cluster nodes
     * @param timeout      the timeout
     */
    public RedisClusterConnPool(final Set<HostAndPort> clusterNodes,
                                final int timeout) {

        this(clusterNodes, timeout, timeout);
    }

    /**
     * Instantiates a new Redis cluster conn pool.
     *
     * @param clusterNodes      the jedis cluster nodes
     * @param connectionTimeout the connection timeout
     * @param soTimeout         the so timeout
     */
    public RedisClusterConnPool(final Set<HostAndPort> clusterNodes,
                                final int connectionTimeout,
                                final int soTimeout) {

        this(clusterNodes, connectionTimeout, soTimeout, RedisConfig.DEFAULT_MAXATTE);
    }

    /**
     * Instantiates a new Redis cluster conn pool.
     *
     * @param clusterNodes      the jedis cluster nodes
     * @param connectionTimeout the connection timeout
     * @param soTimeout         the so timeout
     * @param maxAttempts       the max attempts
     */
    public RedisClusterConnPool(final Set<HostAndPort> clusterNodes,
                                final int connectionTimeout,
                                final int soTimeout,
                                final int maxAttempts) {

        this(clusterNodes, connectionTimeout, soTimeout, maxAttempts, new ConnectionPoolConfig());
    }

    /**
     * Instantiates a new Redis cluster conn pool.
     *
     * @param properties the properties
     */
    public RedisClusterConnPool(final Properties properties) {

        this(new ConnectionPoolConfig(), properties);
    }

    /**
     * Instantiates a new Redis cluster conn pool.
     *
     * @param poolConfig the pool config
     * @param properties the properties
     */
    public RedisClusterConnPool(final ConnectionPoolConfig poolConfig, final Properties properties) {

        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();

        for (String hostAndPort : properties.getProperty(RedisConfig.CLUSTER_PROPERTY).split(","))

            jedisClusterNodes.add(new HostAndPort(hostAndPort.split(":")[0], Integer.valueOf(hostAndPort.split(":")[1])));

        int timeout = Integer.parseInt(properties.getProperty(RedisConfig.TIMEOUT_PROPERTY, String.valueOf(RedisConfig.DEFAULT_TIMEOUT)));

        int maxAttempts = Integer.valueOf(properties.getProperty(RedisConfig.MAXATTE_PROPERTY, String.valueOf(RedisConfig.DEFAULT_MAXATTE)));

        String password = properties.getProperty(RedisConfig.PASSWORD_PROPERTY, RedisConfig.DEFAULT_PASSWORD);

        jedisCluster = new JedisCluster(jedisClusterNodes, timeout, timeout, maxAttempts, password, poolConfig);
    }

    /**
     * Instantiates a new Redis cluster conn pool.
     *
     * @param poolConfig   the pool config
     * @param clusterNodes the cluster nodes
     */
    public RedisClusterConnPool(final ConnectionPoolConfig poolConfig, final Set<HostAndPort> clusterNodes) {

        this(clusterNodes, RedisConfig.DEFAULT_TIMEOUT, RedisConfig.DEFAULT_MAXATTE, poolConfig);
    }

    /**
     * Instantiates a new Redis cluster conn pool.
     *
     * @param clusterNodes the cluster nodes
     * @param timeout      the timeout
     * @param maxAttempts  the max attempts
     * @param poolConfig   the pool config
     */
    public RedisClusterConnPool(final Set<HostAndPort> clusterNodes,
                                final int timeout,
                                final int maxAttempts,
                                final ConnectionPoolConfig poolConfig) {

        this(clusterNodes, timeout, timeout, maxAttempts, RedisConfig.DEFAULT_PASSWORD, poolConfig);
    }

    /**
     * Instantiates a new Redis cluster conn pool.
     *
     * @param clusterNodes      the jedis cluster nodes
     * @param connectionTimeout the connection timeout
     * @param soTimeout         the so timeout
     * @param maxAttempts       the max attempts
     * @param poolConfig        the pool config
     */
    public RedisClusterConnPool(final Set<HostAndPort> clusterNodes,
                                final int connectionTimeout,
                                final int soTimeout,
                                final int maxAttempts,
                                final ConnectionPoolConfig poolConfig) {

        this(clusterNodes, connectionTimeout, soTimeout, maxAttempts, RedisConfig.DEFAULT_PASSWORD, poolConfig);
    }

    /**
     * Instantiates a new Redis cluster conn pool.
     *
     * @param clusterNodes      the cluster nodes
     * @param connectionTimeout the connection timeout
     * @param soTimeout         the so timeout
     * @param maxAttempts       the max attempts
     * @param password          the password
     * @param poolConfig        the pool config
     */
    public RedisClusterConnPool(final Set<HostAndPort> clusterNodes,
                                final int connectionTimeout,
                                final int soTimeout,
                                final int maxAttempts,
                                final String password,
                                final ConnectionPoolConfig poolConfig) {

        jedisCluster = new JedisCluster(clusterNodes, connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
    }

    @Override
    public JedisCluster getConnection() {

        return jedisCluster;
    }

    @Override
    public void returnConnection(JedisCluster conn) {
        // nothing to do...
    }

    @Override
    public void invalidateConnection(JedisCluster conn) {
        // nothing to do...
    }

    /**
     * Close.
     *
     * @throws IOException the io exception
     */
    public void close() throws IOException {

        jedisCluster.close();
    }
}
