/**
 * Copyright 2014 Nirmata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nirmata.workflow;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public abstract class BaseForTests {
    protected static String KAFKA_ADDR = "localhost:9092";
    protected static String MONGO_URI = "mongodb://localhost:27017";
    protected static String NAMESPACE = "testns";
    protected static String NAMESPACE_VER = "v1";
    protected static boolean runKafkaTests = true;
    protected static boolean useMongo = true;

    private final Logger log = LoggerFactory.getLogger(getClass());
    protected TestingServer server;
    protected CuratorFramework curator;
    protected final Timing timing = new Timing();

    static {
        if (System.getProperty("kafka.test.enable", "true").equalsIgnoreCase("false")) {
            runKafkaTests = false;
        }
        if (System.getProperty("mongo.test.enable", "true").equalsIgnoreCase("false")) {
            useMongo = false;
        }
    }

    @BeforeMethod
    public void setup() throws Exception {
        server = new TestingServer();

        curator = CuratorFrameworkFactory.builder().connectString(server.getConnectString())
                .retryPolicy(new RetryOneTime(1)).build();
        curator.start();
    }

    @AfterMethod
    public void teardown() throws Exception {
        CloseableUtils.closeQuietly(curator);
        CloseableUtils.closeQuietly(server);
    }

    protected WorkflowManagerKafkaBuilder createWorkflowKafkaBuilder() {
        try {
            WorkflowManagerKafkaBuilder builder = WorkflowManagerKafkaBuilder.builder()
                    .withKafka(KAFKA_ADDR, NAMESPACE, NAMESPACE_VER);
            if (useMongo) {
                builder = builder.withMongo(MONGO_URI, NAMESPACE, NAMESPACE_VER);
            }
            return builder;
        } catch (Exception e) {
            log.error("Could not create workflow manager with kafka and Mongo", e);
            throw e;
        }
    }

}
