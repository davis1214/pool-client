package com.le.pool;


import com.le.pool.tool.ConnectionPoolConfig;
import org.junit.Assert;
import org.junit.Test;

public class PoolConfigTest {

    @Test
    public void test() throws Exception {

        ConnectionPoolConfig config = new ConnectionPoolConfig();

        Assert.assertNotNull(config);

        Assert.assertEquals(ConnectionPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS, 60000);

        Assert.assertEquals(ConnectionPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS, 30000);

        Assert.assertEquals(ConnectionPoolConfig.DEFAULT_TEST_WHILE_IDLE, true);

        Assert.assertEquals(ConnectionPoolConfig.DEFAULT_NUM_TESTS_PER_EVICTION_RUN, -1);
    }
}
