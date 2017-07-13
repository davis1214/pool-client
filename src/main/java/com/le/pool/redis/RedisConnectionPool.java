package com.le.pool.redis;

import com.le.pool.tool.ConnectionPool;
import com.le.pool.tool.ConnectionPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Properties;

public class RedisConnectionPool implements ConnectionPool<Jedis> {

    private final JedisPool pool;

    /**
     * Instantiates a new Redis connection pool.
     *
     * @param host the host
     * @param port the port
     */
    public RedisConnectionPool(final String host, final int port) {

        this(new ConnectionPoolConfig(), host, port);
    }

    /**
     * Instantiates a new Redis connection pool.
     *
     * @param poolConfig the pool config
     * @param host       the host
     * @param port       the port
     */
    public RedisConnectionPool(final ConnectionPoolConfig poolConfig, final String host, final int port) {

        this(poolConfig, host, port,
                RedisConfig.DEFAULT_TIMEOUT,
                RedisConfig.DEFAULT_PASSWORD,
                RedisConfig.DEFAULT_DATABASE,
                RedisConfig.DEFAULT_CLIENTNAME);
    }

    /**
     * Instantiates a new Redis connection pool.
     *
     * @param properties the properties
     */
    public RedisConnectionPool(final Properties properties) {

        this(new ConnectionPoolConfig(), properties);
    }

    /**
     * Instantiates a new Redis connection pool.
     *
     * @param poolConfig the pool config
     * @param properties the properties
     */
    public RedisConnectionPool(final ConnectionPoolConfig poolConfig, final Properties properties) {

        this(poolConfig,
                properties.getProperty(RedisConfig.ADDRESS_PROPERTY).split(":")[0],
                Integer.parseInt(properties.getProperty(RedisConfig.ADDRESS_PROPERTY).split(":")[1]),
                Integer.parseInt(properties.getProperty(RedisConfig.TIMEOUT_PROPERTY, String.valueOf(RedisConfig.DEFAULT_TIMEOUT))),
                properties.getProperty(RedisConfig.PASSWORD_PROPERTY),
                Integer.parseInt(properties.getProperty(RedisConfig.DATABASE_PROPERTY, String.valueOf(RedisConfig.DEFAULT_DATABASE))),
                properties.getProperty(RedisConfig.CLIENTNAME_PROPERTY));

    }

    /**
     * Instantiates a new Redis connection pool.
     *
     * @param poolConfig the pool config
     * @param host       the host
     * @param port       the port
     * @param timeout    the timeout
     * @param password   the password
     * @param database   the database
     * @param clientName the client name
     */
    public RedisConnectionPool(final ConnectionPoolConfig poolConfig,
                               final String host,
                               final int port,
                               final int timeout,
                               final String password,
                               final int database,
                               final String clientName) {

        this(poolConfig, host, port, timeout, timeout, password, database, clientName);
    }

    /**
     * Instantiates a new Redis connection pool.
     *
     * @param poolConfig        the pool config
     * @param host              the host
     * @param port              the port
     * @param connectionTimeout the connection timeout
     * @param soTimeout         the so timeout
     * @param password          the password
     * @param database          the database
     * @param clientName        the client name
     */
    public RedisConnectionPool(final ConnectionPoolConfig poolConfig,
                               final String host,
                               final int port,
                               final int connectionTimeout,
                               final int soTimeout,
                               final String password,
                               final int database,
                               final String clientName) {

        this.pool = new JedisPool(poolConfig, host, port, connectionTimeout, soTimeout, password, database, clientName, false, null, null, null);
    }

    @Override
    public Jedis getConnection() {

        return pool.getResource();
    }

    @Override
    public void returnConnection(Jedis conn) {

        if (conn != null)

            conn.close();
    }

    @Override
    public void invalidateConnection(Jedis conn) {

        if (conn != null)

            conn.close();
    }

    /**
     * Close.
     */
    public void close() {

        pool.close();
    }
}
