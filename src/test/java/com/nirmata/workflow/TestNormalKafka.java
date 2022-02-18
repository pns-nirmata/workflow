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

import com.google.common.io.Resources;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.admin.RunInfo;
import com.nirmata.workflow.admin.TaskDetails;
import com.nirmata.workflow.admin.TaskInfo;
import com.nirmata.workflow.admin.WorkflowAdmin;
import com.nirmata.workflow.admin.WorkflowManagerState;
import com.nirmata.workflow.details.WorkflowManagerKafkaImpl;
import com.nirmata.workflow.serialization.JsonSerializerMapper;

import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class TestNormalKafka {
    protected Properties kafkaProps = new Properties();
    protected final Timing timing = new Timing();

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Test
    public void testSingleClientSimple() throws Exception {
        TestTaskExecutor taskExecutor = new TestTaskExecutor(6);
        WorkflowManager workflowManager = WorkflowManagerKafkaBuilder.builder()
                .addingTaskExecutor(taskExecutor, 10, new TaskType("test", "1", true))
                .withKafka("localhost:9092", "abc#$%a.b_c-d", "v1")
                .withMongo("mongodb://localhost:27017", "testns", "v1")
                .build();
        try {

            workflowManager.start();
            WorkflowManagerStateSampler sampler = new WorkflowManagerStateSampler(workflowManager.getAdmin(), 10,
                    Duration.ofMillis(100));
            sampler.start();

            timing.sleepABit();

            String json = Resources.toString(Resources.getResource("tasks.json"), Charset.defaultCharset());
            JsonSerializerMapper jsonSerializerMapper = new JsonSerializerMapper();
            Task task = jsonSerializerMapper.get(jsonSerializerMapper.getMapper().readTree(json), Task.class);
            RunId runId = workflowManager.submitTask(task);

            WorkflowAdmin wfAdmin = workflowManager.getAdmin();
            List<RunId> runIds = wfAdmin.getRunIds();
            List<RunInfo> runInfos = wfAdmin.getRunInfo();
            RunInfo runInfo = wfAdmin.getRunInfo(runId);
            Map<TaskId, TaskDetails> taskDetails = wfAdmin.getTaskDetails(runId);
            List<TaskInfo> taskInfo = wfAdmin.getTaskInfo(runId);

            taskExecutor.getLatch().await();
            // Give Kafka some time to autocommit
            Thread.sleep(5000); // timing.sleepABit();

            // TODO: Have relevant asserts for the following
            // when we run with MongoDB.
            wfAdmin = workflowManager.getAdmin();
            runIds = wfAdmin.getRunIds();
            runInfos = wfAdmin.getRunInfo();
            runInfo = wfAdmin.getRunInfo(runId);
            taskDetails = wfAdmin.getTaskDetails(runId);
            taskInfo = wfAdmin.getTaskInfo(runId);
            log.debug("{}, {}, {}, {}, {}", runIds, runInfos, runInfo, taskDetails, taskInfo);

            List<TaskId> flatSet = new ArrayList<TaskId>();
            for (Set<TaskId> set : taskExecutor.getChecker().getSets()) {
                flatSet.addAll(
                        set.stream().sorted((i1, i2) -> i1.getId().compareTo(i2.getId())).collect(Collectors.toList()));
            }
            List<TaskId> expectedSets = Arrays.<TaskId>asList(new TaskId("task1"), new TaskId("task2"),
                    new TaskId("task3"), new TaskId("task4"), new TaskId("task5"),
                    new TaskId("task6"));
            Assert.assertEquals(flatSet, expectedSets);

            taskExecutor.getChecker().assertNoDuplicates();

            sampler.close();
            log.info("Samples {}", sampler.getSamples());
        } catch (Exception e) {
            log.error("Unexpected exception: ", e);
        } finally {
            closeWorkflow(workflowManager);
        }
    }

    private void closeWorkflow(WorkflowManager workflowManager) throws InterruptedException {
        CloseableUtils.closeQuietly(workflowManager);
        timing.sleepABit();
        ((WorkflowManagerKafkaImpl) workflowManager).debugValidateClosed();
    }

}
