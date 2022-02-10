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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nirmata.workflow.WorkflowManager;
import com.nirmata.workflow.admin.RunInfo;
import com.nirmata.workflow.admin.TaskDetails;
import com.nirmata.workflow.admin.TaskInfo;
import com.nirmata.workflow.admin.WorkflowAdmin;
import com.nirmata.workflow.admin.WorkflowManagerState;
import com.nirmata.workflow.details.internalmodels.RunnableTask;
import com.nirmata.workflow.details.internalmodels.StartedTask;
import com.nirmata.workflow.details.internalmodels.WorkflowMessage;
import com.nirmata.workflow.events.WorkflowListenerManager;
import com.nirmata.workflow.executor.TaskExecution;
import com.nirmata.workflow.executor.TaskExecutor;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.Task;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.QueueConsumer;
import com.nirmata.workflow.queue.QueueFactory;
import com.nirmata.workflow.serialization.Serializer;

import org.apache.curator.utils.CloseableUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class WorkflowManagerKafkaImpl implements WorkflowManager, WorkflowAdmin {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final String instanceName;
    private final List<QueueConsumer> consumers;
    private KafkaHelper kafkaHelper;
    private final boolean workflowWorkerEnabled;
    Producer<String, byte[]> wflowProducer;
    Producer<String, byte[]> execResultProducer;

    private final SchedulerSelectorKafka schedulerSelector;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final Serializer serializer;
    private final Executor taskRunnerService;

    private static final TaskType nullTaskType = new TaskType("", "", false);

    private enum State {
        LATENT,
        STARTED,
        CLOSED
    }

    public WorkflowManagerKafkaImpl(KafkaHelper kafkaConf, boolean workflowWorkerEnabled, QueueFactory queueFactory,
            String instanceName, List<TaskExecutorSpec> specs, AutoCleanerHolder autoCleanerHolder,
            Serializer serializer, Executor taskRunnerService) {
        this.taskRunnerService = Preconditions.checkNotNull(taskRunnerService, "taskRunnerService cannot be null");
        this.serializer = Preconditions.checkNotNull(serializer, "serializer cannot be null");
        autoCleanerHolder = Preconditions.checkNotNull(autoCleanerHolder, "autoCleanerHolder cannot be null");

        this.kafkaHelper = Preconditions.checkNotNull(kafkaConf, "kafka props cannot be null");
        this.workflowWorkerEnabled = workflowWorkerEnabled;
        wflowProducer = new KafkaProducer<String, byte[]>(this.kafkaHelper.getProducerProps());
        execResultProducer = new KafkaProducer<String, byte[]>(this.kafkaHelper.getProducerProps());

        queueFactory = Preconditions.checkNotNull(queueFactory, "queueFactory cannot be null");
        this.instanceName = Preconditions.checkNotNull(instanceName, "instanceName cannot be null");
        specs = Preconditions.checkNotNull(specs, "specs cannot be null");

        consumers = makeTaskConsumers(queueFactory, specs);
        schedulerSelector = new SchedulerSelectorKafka(this, queueFactory, autoCleanerHolder);
    }

    public KafkaHelper getKafkaConf() {
        return this.kafkaHelper;
    }

    @VisibleForTesting
    volatile boolean debugDontStartConsumers = false;

    @Override
    public void start() {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        if (!debugDontStartConsumers) {
            startQueueConsumers();
        }
        if (workflowWorkerEnabled) {
            kafkaHelper.createWorkflowTopicIfNeeded();
            schedulerSelector.start();
        }
    }

    @VisibleForTesting
    void startQueueConsumers() {
        consumers.forEach(QueueConsumer::start);
    }

    @Override
    public WorkflowListenerManager newWorkflowListenerManager() {
        // TODO PNS: Unsupported right now. Provide support for Kafka workflow listener.
        // Currently this interface is not used by any client service.
        // The Kafka workflow can write status of completed runs to a Kafka topic
        // called completed runs and this listener implementation would involve
        // listening to that. Need new listener manager equivalent to
        // WorkflowListenerManagerImpl using Zkp
        throw new UnsupportedOperationException("Listeners on Kafka workflows not yet supported");
    }

    @Override
    public Map<TaskId, TaskDetails> getTaskDetails(RunId runId) {
        // TODO PNS: Dummy return for now. Need to get from DB
        // See equivalent function in Zkp impl
        return new HashMap<TaskId, TaskDetails>();
    }

    @Override
    public RunId submitTask(Task task) {
        return submitSubTask(new RunId(), null, task);
    }

    @Override
    public RunId submitTask(RunId runId, Task task) {
        return submitSubTask(runId, null, task);
    }

    @Override
    public RunId submitSubTask(RunId parentRunId, Task task) {
        return submitSubTask(new RunId(), parentRunId, task);
    }

    public volatile long debugLastSubmittedTimeMs = 0;

    @Override
    public RunId submitSubTask(RunId runId, RunId parentRunId, Task task) {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");

        RunnableTaskDagBuilder builder = new RunnableTaskDagBuilder(task);
        Map<TaskId, ExecutableTask> tasks = builder
                .getTasks()
                .values()
                .stream()
                .collect(Collectors.toMap(Task::getTaskId, t -> new ExecutableTask(runId, t.getTaskId(),
                        t.isExecutable() ? t.getTaskType() : nullTaskType, t.getMetaData(), t.isExecutable())));
        RunnableTask runnableTask = new RunnableTask(tasks, builder.getEntries(), LocalDateTime.now(), null,
                parentRunId);

        try {
            WorkflowMessage wm = new WorkflowMessage(runnableTask);
            byte[] runnableTaskBytes = serializer.serialize(wm);
            debugLastSubmittedTimeMs = System.currentTimeMillis();
            sendWorkflowToKafka(runId, runnableTaskBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return runId;
    }

    // Put the workflow dag in Kafka
    private void sendWorkflowToKafka(RunId runId, byte[] runnableTaskBytes) {
        wflowProducer.send(
                new ProducerRecord<String, byte[]>(
                        kafkaHelper.getWorkflowTopic(),
                        runId.getId(),
                        runnableTaskBytes),
                new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata m, Exception e) {
                        if (e != null) {
                            log.error("Error creating record for Run {} to topic {}", runId,
                                    kafkaHelper.getWorkflowTopic(), e);
                        } else {
                            log.debug("RunId {} produced record to topic {}, partition [{}] @ offset {}", runId,
                                    m.topic(), m.partition(), m.offset());
                        }
                    }
                });

    }

    public void updateTaskProgress(RunId runId, TaskId taskId, int progress) {
        Preconditions.checkArgument((progress >= 0) && (progress <= 100), "progress must be between 0 and 100");

        // TODO PNS: Update task progress. See equivalent Zkp implementation
    }

    @Override
    public boolean cancelRun(RunId runId) {
        log.info("Attempting to cancel run " + runId);

        // TODO PNS: Send kafka message to scheduler to complete runnable task, new
        // message type
        return true;
    }

    @Override
    public Optional<TaskExecutionResult> getTaskExecutionResult(RunId runId, TaskId taskId) {
        // TODO PNS: Get task execution result from DB
        return Optional.empty();
    }

    public String getInstanceName() {
        return instanceName;
    }

    @VisibleForTesting
    public void debugValidateClosed() {
        consumers.forEach(QueueConsumer::debugValidateClosed);
        schedulerSelector.debugValidateClosed();
    }

    @Override
    public void close() {
        if (state.compareAndSet(State.STARTED, State.CLOSED)) {
            CloseableUtils.closeQuietly(schedulerSelector);
            consumers.forEach(CloseableUtils::closeQuietly);
        }
    }

    @Override
    public WorkflowAdmin getAdmin() {
        return this;
    }

    @Override
    public WorkflowManagerState getWorkflowManagerState() {
        return new WorkflowManagerState(
                false, // Zkp is NA. And it is not continuously connected to Kafka.
                schedulerSelector.getState(),
                consumers.stream().map(QueueConsumer::getState).collect(Collectors.toList()));
    }

    @Override
    public boolean clean(RunId runId) {
        // PNS TODO: Clean up from DB, see Zkp implementation
        return true;
    }

    @Override
    public RunInfo getRunInfo(RunId runId) {
        // PNS TODO: Get run info from DB. Implement
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public List<RunId> getRunIds() {
        // PNS TODO: Get run Ids from DB. Implement.
        // Zkp could not get particular ID, hence needed all.
        // Instead of getRunIds and iterating, one can call getRunId(id)
        return Collections.emptyList();
    }

    @Override
    public List<RunInfo> getRunInfo() {
        // PNS TODO: Get run info from DB. Implement.
        // Just like getRunIds above, better to call getRunInfo for a given Id
        // since DB supports it.
        return Collections.emptyList();
    }

    @Override
    public List<TaskInfo> getTaskInfo(RunId runId) {
        // PNS TODO: Get run info from DB. Implement.
        // Just like getRunIds above, better to call getRunInfo for a given Id
        // since DB supports it.
        return Collections.emptyList();
    }

    public Serializer getSerializer() {
        return serializer;
    }

    @VisibleForTesting
    SchedulerSelectorKafka getSchedulerSelector() {
        return schedulerSelector;
    }

    private void executeTask(TaskExecutor taskExecutor, ExecutableTask executableTask,
            Producer<String, byte[]> taskResultProducer) {
        if (state.get() != State.STARTED) {
            return;
        }

        log.debug("Executing task: {}", executableTask);
        TaskExecution taskExecution = taskExecutor.newTaskExecution(this, executableTask);

        TaskExecutionResult result;
        try {
            FutureTask<TaskExecutionResult> futureTask = new FutureTask<>(taskExecution::execute);
            taskRunnerService.execute(futureTask);
            result = futureTask.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (ExecutionException e) {
            log.error("Could not execute task: " + executableTask, e);
            throw new RuntimeException(e);
        }
        if (result == null) {
            throw new RuntimeException(String.format("null returned from task executor for run: %s, task %s",
                    executableTask.getRunId(), executableTask.getTaskId()));
        }
        byte[] bytes = serializer.serialize(new WorkflowMessage(executableTask.getTaskId(), result));
        try {
            taskResultProducer.send(
                    new ProducerRecord<String, byte[]>(
                            kafkaHelper.getWorkflowTopic(),
                            executableTask.getRunId().getId(),
                            bytes),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata m, Exception e) {
                            if (e != null) {
                                log.error("Error creating record for Run {} to topic {}", executableTask.getRunId(),
                                        kafkaHelper.getWorkflowTopic(), e);
                            } else {
                                log.debug("RunId {} produced record to result topic {}, partition [{}] @ offset {}",
                                        executableTask.getRunId(),
                                        m.topic(), m.partition(), m.offset());
                            }
                        }
                    });
            // TODO PNS: Also record to DB. Send async save call

        } catch (Exception e) {
            log.error("Could not set completed data for executable task: " + executableTask, e);
            throw new RuntimeException(e);
        }
    }

    private List<QueueConsumer> makeTaskConsumers(QueueFactory queueFactory, List<TaskExecutorSpec> specs) {
        ImmutableList.Builder<QueueConsumer> builder = ImmutableList.builder();
        specs.forEach(spec -> IntStream.range(0, spec.getQty()).forEach(i -> {

            QueueConsumer consumer = queueFactory.createQueueConsumer(this,
                    t -> executeTask(spec.getTaskExecutor(), t, this.execResultProducer),
                    spec.getTaskType());
            builder.add(consumer);
        }));

        return builder.build();
    }
}
