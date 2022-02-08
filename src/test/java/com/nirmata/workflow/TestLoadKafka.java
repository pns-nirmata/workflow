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

import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.details.KafkaHelper;
import com.nirmata.workflow.details.WorkflowManagerKafkaImpl;

import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestLoadKafka extends TestLoadBase {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private static final String KAFKA_NS = "testkafkans";
    private static final String KAFKA_NS_VER = "v1";
    private static final String TASKTYPE = "test";
    private static final String TASKTYPE_VER = "1";

    @BeforeMethod
    public void setup() throws Exception {
        KafkaHelper helper = new KafkaHelper("localhost:9092", KAFKA_NS, KAFKA_NS_VER);
        helper.deleteWorkflowTopic();
        helper.deleteTaskTopic(new TaskType(TASKTYPE, TASKTYPE_VER, true));
        Thread.sleep(5000);
    }

    @AfterMethod
    public void teardown() throws Exception {
        KafkaHelper helper = new KafkaHelper("localhost:9092", KAFKA_NS, KAFKA_NS_VER);
        helper.deleteWorkflowTopic();
        helper.deleteTaskTopic(new TaskType(TASKTYPE, TASKTYPE_VER, true));
    }

    @Test
    public void testLoadKafka1() throws Exception {
        TestTaskExecutor taskExecutor = new TestTaskExecutor(getTest1Tasks(), false, getTest1Delay());
        WorkflowManager workflowManager = WorkflowManagerKafkaBuilder.builder()
                .addingTaskExecutor(taskExecutor, 10, new TaskType(TASKTYPE, "1", true))
                .withKafka("localhost:9092", KAFKA_NS, KAFKA_NS_VER)
                .build();
        try {
            super.testLoad1(workflowManager, taskExecutor);
        } catch (Exception e) {
            log.error("Unexpected exception: ", e);
        } finally {
            closeWorkflow(workflowManager);
        }
    }

    protected void closeWorkflow(WorkflowManager workflowManager) throws InterruptedException {
        // Give some time to cleanup workflow manager state (E.g. Kafka autocommit)
        Thread.sleep(5000); // timing.sleepABit();

        CloseableUtils.closeQuietly(workflowManager);
        timing.sleepABit();
        ((WorkflowManagerKafkaImpl) workflowManager).debugValidateClosed();
    }

}
