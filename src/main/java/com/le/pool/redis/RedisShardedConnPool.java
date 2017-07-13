package com.le.pool.redis;

import com.le.pool.tool.ConnectionPoolConfig;
import com.le.pool.tool.ConnectionPool;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.util.Hashing;

import java.util.List;
import java.util.regex.Pattern;

public class RedisShardedConnPool implements ConnectionPool<ShardedJedis> {

    private final ShardedJedisPool pool;

    /**
     * Instantiates a new Redis sharded conn pool.
     *
     * @param poolConfig the pool config
     * @param shards     the shards
     */
    public RedisShardedConnPool(final ConnectionPoolConfig poolConfig,
                                final List<JedisShardInfo> shards) {

        this(poolConfig, shards, Hashing.MURMUR_HASH);
    }

    /**
     * Instantiates a new Redis sharded conn pool.
     *
     * @param poolConfig the pool config
     * @param shards     the shards
     * @param algo       the algo
     */
    public RedisShardedConnPool(final ConnectionPoolConfig poolConfig,
                                final List<JedisShardInfo> shards,
                                final Hashing algo) {

        this(poolConfig, shards, algo, null);
    }

    /**
     * Instantiates a new Redis sharded conn pool.
     *
     * @param poolConfig    the pool config
     * @param shards        the shards
     * @param keyTagPattern the key tag pattern
     */
    public RedisShardedConnPool(final ConnectionPoolConfig poolConfig,
                                final List<JedisShardInfo> shards,
                                final Pattern keyTagPattern) {

        this(poolConfig, shards, Hashing.MURMUR_HASH, keyTagPattern);
    }

    /**
     * Instantiates a new Redis sharded conn pool.
     *
     * @param poolConfig    the pool config
     * @param shards        the shards
     * @param algo          the algo
     * @param keyTagPattern the key tag pattern
     */
    public RedisShardedConnPool(final ConnectionPoolConfig poolConfig,
                                final List<JedisShardInfo> shards,
                                final Hashing algo,
                                final Pattern keyTagPattern) {

        this.pool = new ShardedJedisPool(poolConfig, shards, algo, keyTagPattern);
    }

    @Override
    public ShardedJedis getConnection() {

        return pool.getResource();
    }

    @Override
    public void returnConnection(ShardedJedis conn) {

        if (conn != null)

            conn.close();
    }

    @Override
    public void invalidateConnection(ShardedJedis conn) {

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
