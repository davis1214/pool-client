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
package com.le.pool.hbase;

import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

public class HbaseConnectionFactoryTest {

    @Test
    public void test_0() throws Exception {

        try {
            new HbaseConnectionFactory(null, null, null, null);

        } catch (Exception e) {
        }

        try {
            new HbaseConnectionFactory(HbaseConfig.DEFAULT_HOST, null, null,
                    null);

        } catch (Exception e) {
        }

        try {
            new HbaseConnectionFactory(new Configuration());

        } catch (Exception e) {
        }

        try {
            new HbaseConnectionFactory(new Properties());

        } catch (Exception e) {
        }

        Properties prop = new Properties();

        try {
            prop.setProperty(HbaseConfig.ZOOKEEPER_QUORUM_PROPERTY,
                    HbaseConfig.DEFAULT_HOST);
            new HbaseConnectionFactory(prop);

            prop.setProperty(HbaseConfig.MASTER_PROPERTY, "localhost:60000");
            prop.setProperty(HbaseConfig.ROOTDIR_PROPERTY,
                    "hdfs://localhost:8020/hbase");
            new HbaseConnectionFactory(prop);

        } catch (Exception e) {
        }

        try {
            prop.setProperty(HbaseConfig.ZOOKEEPER_CLIENTPORT_PROPERTY,
                    HbaseConfig.DEFAULT_PORT);
            new HbaseConnectionFactory(prop);
        } catch (Exception e) {
        }

        try {
            prop.setProperty(HbaseConfig.MASTER_PROPERTY, "localhost:60000");
            prop.setProperty(HbaseConfig.ROOTDIR_PROPERTY,
                    "hdfs://localhost:8020/hbase");
            new HbaseConnectionFactory(prop);
        } catch (Exception e) {
        }

        HbaseConnectionFactory factory = new HbaseConnectionFactory(
                HbaseConfig.DEFAULT_HOST, HbaseConfig.DEFAULT_PORT,
                "localhost:60000", "hdfs://localhost:8020/hbase");

        try {
            factory.makeObject();
        } catch (Exception e) {
        }

        try {
            factory.activateObject(new DefaultPooledObject<Connection>(null));
        } catch (Exception e) {
        }

        try {
            factory.validateObject(new DefaultPooledObject<Connection>(null));
        } catch (Exception e) {
        }

        try {
            factory.validateObject(new DefaultPooledObject<Connection>(new Conn1()));

        } catch (Exception e) {
        }

        try {
            factory.validateObject(new DefaultPooledObject<Connection>(new Conn2()));

        } catch (Exception e) {
        }

        try {
            factory.validateObject(new DefaultPooledObject<Connection>(new Conn3()));

        } catch (Exception e) {
        }

        try {
            factory.passivateObject(new DefaultPooledObject<Connection>(null));
        } catch (Exception e) {
        }

        try {
            factory.destroyObject(new DefaultPooledObject<Connection>(null));
        } catch (Exception e) {
        }

        try {
            factory.destroyObject(new DefaultPooledObject<Connection>(new Conn1()));
        } catch (Exception e) {
        }
    }

    class Conn1 implements Connection {

        @Override
        public Configuration getConfiguration() {
            return null;
        }

        @Override
        public Table getTable(TableName tableName) throws IOException {
            return null;
        }

        @Override
        public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
            return null;
        }

        @Override
        public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
            return null;
        }

        @Override
        public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
            return null;
        }

        @Override
        public RegionLocator getRegionLocator(TableName tableName) throws IOException {
            return null;
        }

        @Override
        public Admin getAdmin() throws IOException {
            return null;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void abort(String why, Throwable e) {

        }

        @Override
        public boolean isAborted() {
            return false;
        }
    }

    class Conn2 implements Connection {

        @Override
        public Configuration getConfiguration() {
            return null;
        }

        @Override
        public Table getTable(TableName tableName) throws IOException {
            return null;
        }

        @Override
        public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
            return null;
        }

        @Override
        public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
            return null;
        }

        @Override
        public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
            return null;
        }

        @Override
        public RegionLocator getRegionLocator(TableName tableName) throws IOException {
            return null;
        }

        @Override
        public Admin getAdmin() throws IOException {
            return null;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public boolean isClosed() {
            return true;
        }

        @Override
        public void abort(String why, Throwable e) {

        }

        @Override
        public boolean isAborted() {
            return false;
        }
    }

    class Conn3 implements Connection {

        @Override
        public Configuration getConfiguration() {
            return null;
        }

        @Override
        public Table getTable(TableName tableName) throws IOException {
            return null;
        }

        @Override
        public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
            return null;
        }

        @Override
        public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
            return null;
        }

        @Override
        public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
            return null;
        }

        @Override
        public RegionLocator getRegionLocator(TableName tableName) throws IOException {
            return null;
        }

        @Override
        public Admin getAdmin() throws IOException {
            return null;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public boolean isClosed() {
            return true;
        }

        @Override
        public void abort(String why, Throwable e) {

        }

        @Override
        public boolean isAborted() {
            return true;
        }
    }
}
