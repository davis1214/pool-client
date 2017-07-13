package com.le.pool.jdbc;

import com.le.pool.tool.ConnectionPoolConfig;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static java.lang.System.out;

public class JdbcTest {

    @Test
    public void test() throws Exception {

        ConnectionPoolConfig config = new ConnectionPoolConfig();
        config.setMaxTotal(20);
        config.setMaxIdle(5);
        config.setMaxWaitMillis(1000);
        config.setTestOnBorrow(true);

        Properties props = new Properties();
        props.setProperty("driverClass", "com.mysql.jdbc.Driver");
        props.setProperty("jdbcUrl", "jdbc:mysql://localhost:3306/test");
        props.setProperty("username", "root");
        props.setProperty("password", "root");

        JdbcConnectionPool pool = new JdbcConnectionPool(config, props);

        Connection conn = pool.getConnection();

        Statement stat = conn.createStatement();

        ResultSet rs = stat.executeQuery("select ID , name from test1");

        while (rs.next()) {
            int id = rs.getInt("ID");
            String name = rs.getString("NAME");

            out.println("id :" + id + " , name :" + name);
        }

        pool.returnConnection(conn);

        pool.close();
    }
}
