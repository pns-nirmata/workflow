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
package com.nirmata.workflow.details;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.nirmata.workflow.admin.WorkflowManagerState;
import com.nirmata.workflow.details.internalmodels.RunnableTask;
import com.nirmata.workflow.details.internalmodels.StartedTask;
import com.nirmata.workflow.details.internalmodels.WorkflowMessage;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.storage.RunRecord;
import com.nirmata.workflow.storage.StorageManager;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class SchedulerKafka implements Runnable {
    @VisibleForTesting
    static volatile AtomicInteger debugBadRunIdCount;

    private final Duration EXPIRY_MINS = Duration.ofMinutes(120);
    private final int MAX_SUBMITTED_CACHE_ITEMS = 10000;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WorkflowManagerKafkaImpl workflowManager;
    private final StorageManager storageMgr;
    private final AutoCleanerHolder autoCleanerHolder;
    private Map<TaskType, Producer<String, byte[]>> taskQueues = new HashMap<TaskType, Producer<String, byte[]>>();
    private final Consumer<String, byte[]> workflowConsumer;

    private Map<String, Map<String, TaskExecutionResult>> completedTasksCache = new HashMap<String, Map<String, TaskExecutionResult>>();
    private Map<String, Set<String>> startedTasksCache = new HashMap<String, Set<String>>();
    private Map<String, RunnableTask> runsCache = new HashMap<String, RunnableTask>();

    // Sufficiently large LRU cache to ensure that duplicate tasks (with same runId)
    // are not scheduled even if scheduled multiple times within a time interval
    // and even if MongoDB is not used as a store.
    // The max size should be based on duplicate arrival times to be accomodated
    private final Set<String> recentlySubmittedTasks = Collections.newSetFromMap(new LinkedHashMap<String, Boolean>() {
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > MAX_SUBMITTED_CACHE_ITEMS;
        }
    });

    private AtomicReference<WorkflowManagerState.State> state = new AtomicReference<>(
            WorkflowManagerState.State.LATENT);

    // TODO: Later. Zkp implementation takes an additional queue factory.
    // Don't think that level of customization is needed. We simply queue to kafka.
    // Or maybe, evaluate benefits of queue customization later.
    SchedulerKafka(WorkflowManagerKafkaImpl workflowManager,
            AutoCleanerHolder autoCleanerHolder) {
        this.workflowManager = workflowManager;
        this.storageMgr = workflowManager.getStorageManager();
        this.autoCleanerHolder = autoCleanerHolder;

        workflowManager.getKafkaConf().createWorkflowTopicIfNeeded();
        this.workflowConsumer = new KafkaConsumer<String, byte[]>(
                workflowManager.getKafkaConf()
                        .getConsumerProps(workflowManager.getKafkaConf().getWorkflowConsumerGroup()));
    }

    WorkflowManagerState.State getState() {
        return state.get();
    }

    public void run() {
        // One workflow run thread in a client is good enough, but we can
        // parallelize this easily too. Also, many other clients will also offer to act
        // as workflow workers. So parallel processing will happen anyways with multiple
        // partitions for workflows.
        this.workflowConsumer.subscribe(Collections.singletonList(workflowManager.getKafkaConf().getWorkflowTopic()));

        while (true) {
            state.set(WorkflowManagerState.State.SLEEPING);
            ConsumerRecords<String, byte[]> records = workflowConsumer.poll(1000);
            if (records.count() > 0) {
                state.set(WorkflowManagerState.State.PROCESSING);
            } else {
                continue;
            }

            // TODO: Later, incorporate fairness here. We can take ideas from Kubernetes
            // https://kubernetes.io/docs/concepts/cluster-administration/flow-control/

            for (ConsumerRecord<String, byte[]> record : records) {
                // TODO PNS: Improve this loop. Club statements into separate functions
                RunId runId = new RunId(record.key());
                WorkflowMessage msg = workflowManager.getSerializer().deserialize(record.value(),
                        WorkflowMessage.class);
                log.debug("Deserialized message of type {} from partition {} (key: {}) at offset {}",
                        msg.getMsgType(), record.partition(), record.key(), record.offset());

                switch (msg.getMsgType()) {
                    case TASK:
                        if (!msg.isRetry()) {
                            if (!recentlySubmittedTasks.contains(runId.getId())) {
                                completedTasksCache.put(runId.getId(), new HashMap<String, TaskExecutionResult>());
                                startedTasksCache.put(runId.getId(), new HashSet<String>());
                                runsCache.put(runId.getId(), msg.getRunnableTask().get());
                                recentlySubmittedTasks.add(runId.getId());
                            } else {
                                log.debug("Ignoring duplicate task submitted for run {}", runId);
                                continue;
                            }
                        } else {
                            // Someone retrying this workflow. Perhaps some old workflow run died
                            populateCacheFromDb(runId);
                        }

                        break;
                    case TASKRESULT:
                        if (runsCache.containsKey(runId.getId())) {
                            completedTasksCache.get(runId.getId()).put(msg.getTaskId().get().getId(),
                                    msg.getTaskExecResult().get());
                            startedTasksCache.get(runId.getId()).remove(msg.getTaskId().get().getId());
                        } else {
                            // A task result was received for run that I don't have
                            // Mostly some partition reassignment, or cancelled run.
                            // Or wait for someone to resubmit the job
                            log.warn(
                                    "Got result, but no runId for {}, ignoring. Repartition due to failure or residual in Kafka to to late autocommit?",
                                    runId.getId());
                        }
                        break;
                    case CANCEL:
                        try {
                            completeRunnableTask(log, workflowManager, runId,
                                    workflowManager.getSerializer().deserialize(storageMgr.getRunnable(runId),
                                            RunnableTask.class),
                                    -1);
                        } catch (Exception ex) {
                            log.error("Could not find any data to cancel run: {}", runId);
                        }
                        continue;
                    default:
                        log.error("Workflow worker received invalid message type for runId {}, {}", runId,
                                msg.getMsgType());
                        break;
                }
                updateTasks(runId);

                if (autoCleanerHolder.shouldRun()) {
                    autoCleanerHolder.run(workflowManager.getAdmin());
                }
            }
        }

    }

    private void populateCacheFromDb(RunId runId) {
        try {
            RunRecord runRec = storageMgr.getRunDetails(runId);
            if (runRec == null) {
                log.error("Unexpected state. Did not find runId in DB: {}", runId);
                return;
            }

            completedTasksCache.put(runId.getId(), new HashMap<String, TaskExecutionResult>());
            startedTasksCache.put(runId.getId(), new HashSet<String>());

            RunnableTask runnableTask = workflowManager.getSerializer().deserialize(runRec.getRunnableData(),
                    RunnableTask.class);
            runsCache.put(runId.getId(), runnableTask);

            for (Map.Entry<String, byte[]> entry : runRec.getCompletedTasks().entrySet()) {
                TaskId taskId = new TaskId(entry.getKey());

                try {
                    TaskExecutionResult taskExecutionResult = workflowManager.getSerializer().deserialize(
                            entry.getValue(),
                            TaskExecutionResult.class);
                    completedTasksCache.get(runId.getId()).put(taskId.getId(), taskExecutionResult);
                } catch (Exception e) {
                    throw new RuntimeException("Trying to read started task info for task: " + taskId, e);
                }
            }

        } catch (Exception e) {
            log.error("Error creating Runnable from DB record for runId: {}", runId, e);
        }
    }

    private boolean hasCanceledTasks(RunId runId, RunnableTask runnableTask) {
        return runnableTask.getTasks().keySet().stream().anyMatch(taskId -> {
            TaskExecutionResult taskExecutionResult = completedTasksCache.get(runId.getId()).get(taskId.getId());
            if (taskExecutionResult != null) {
                return taskExecutionResult.getStatus().isCancelingStatus();
            }
            return false;
        });
    }

    void completeRunnableTask(Logger log, WorkflowManagerKafkaImpl workflowManager, RunId runId,
            RunnableTask runnableTask, int version) {
        runsCache.remove(runId.getId());
        startedTasksCache.remove(runId.getId());
        completedTasksCache.remove(runId.getId());

        log.info("Completing run: {}", runId);
        try {
            RunId parentRunId = runnableTask.getParentRunId().orElse(null);
            RunnableTask completedRunnableTask = new RunnableTask(runnableTask.getTasks(), runnableTask.getTaskDags(),
                    runnableTask.getStartTimeUtc(), LocalDateTime.now(Clock.systemUTC()), parentRunId);
            byte[] json = workflowManager.getSerializer().serialize(completedRunnableTask);
            storageMgr.updateRun(runId, json);
        } catch (Exception e) {
            String message = "Could not write completed task data for run: " + runId;
            log.error(message, e);
            throw new RuntimeException(message, e);
        }
    }

    private void updateTasks(RunId runId) {
        log.debug("Updating run: " + runId);

        RunnableTask runnableTask = getRunnableTask(runId);

        if (runnableTask == null) {
            log.warn("No runnable task for runId {}, skipping update", runId);
            return;
        }

        if (runnableTask.getCompletionTimeUtc().isPresent()) {
            log.debug("Run is completed. Ignoring: " + runId);
            return;
        }

        if (hasCanceledTasks(runId, runnableTask)) {
            log.debug("Run has canceled tasks and will be marked completed: {}", runId);
            completeRunnableTask(log, workflowManager, runId, runnableTask, -1);
            return; // one or more tasks have canceled the entire run
        }

        Set<TaskId> completedTasksForRun = Sets.newHashSet();
        runnableTask.getTaskDags().forEach(entry -> {
            TaskId taskId = entry.getTaskId();
            ExecutableTask task = runnableTask.getTasks().get(taskId);
            if (task == null) {
                log.error(String.format("Could not find task: %s for run: %s", taskId, runId));
                return;
            }

            boolean taskIsComplete = taskIsComplete(runId, task);
            if (taskIsComplete) {
                completedTasksForRun.add(taskId);
            } else if (!taskIsStarted(runId, taskId)) {
                boolean allDependenciesAreComplete = entry
                        .getDependencies()
                        .stream()
                        .allMatch(id -> taskIsComplete(runId, runnableTask.getTasks().get(id)));
                if (allDependenciesAreComplete) {
                    queueTask(runId, task);
                }
            }
        });

        if (completedTasksForRun.equals(runnableTask.getTasks().keySet())) {
            completeRunnableTask(log, workflowManager, runId, runnableTask, -1);
        }
    }

    private RunnableTask getRunnableTask(RunId runId) {
        return runsCache.get(runId.getId());
    }

    private void queueTask(RunId runId, ExecutableTask task) {
        try {
            // TODO: Later, Incorporate delayed tasks here somehow??.

            StartedTask startedTask = new StartedTask(workflowManager.getInstanceName(),
                    LocalDateTime.now(Clock.systemUTC()), 0);
            storageMgr.setStartedTask(runId, task.getTaskId(), workflowManager.getSerializer().serialize(startedTask));

            byte[] runnableTaskBytes = workflowManager.getSerializer().serialize(task);

            Producer<String, byte[]> producer = taskQueues.get(task.getTaskType());
            if (producer == null) {
                workflowManager.getKafkaConf().createTaskTopicIfNeeded(task.getTaskType());
                producer = new KafkaProducer<String, byte[]>(
                        workflowManager.getKafkaConf().getProducerProps());
                taskQueues.put(task.getTaskType(), producer);
            }

            producer.send(new ProducerRecord<String, byte[]>(
                    workflowManager.getKafkaConf().getTaskExecTopic(task.getTaskType()), runnableTaskBytes),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata m, Exception e) {
                            if (e != null) {
                                log.error("Error creating record for Run {} to task type {}", runId, task.getTaskType(),
                                        e);
                            } else {
                                startedTasksCache.get(runId.getId()).add(task.getTaskId().getId());
                                log.debug("RunId {} produced record to topic {}, partition [{}] @ offset {}", runId,
                                        m.topic(), m.partition(), m.offset());
                            }
                        }
                    });
            log.debug("Sent task to queue: {}", task);
        } catch (Exception e) {
            String message = "Could not start task " + task;
            log.error(message, e);
            throw new RuntimeException(e);
        }
    }

    private boolean taskIsStarted(RunId runId, TaskId taskId) {
        return startedTasksCache.get(runId.getId()).contains(taskId.getId());
    }

    private boolean taskIsComplete(RunId runId, ExecutableTask task) {
        if ((task == null) || !task.isExecutable()) {
            return true;
        }

        TaskExecutionResult result = completedTasksCache.get(runId.getId()).get(task.getTaskId().getId());
        if (result != null) {
            if (result.getSubTaskRunId().isPresent()) {
                RunnableTask runnableTask = getRunnableTask(result.getSubTaskRunId().get());
                return (runnableTask != null) && runnableTask.getCompletionTimeUtc().isPresent();
            }
            return true;
        }
        return false;
    }
}
