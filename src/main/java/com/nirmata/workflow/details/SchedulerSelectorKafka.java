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
import com.nirmata.workflow.admin.WorkflowManagerState;
import com.nirmata.workflow.queue.QueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class SchedulerSelectorKafka implements Closeable {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WorkflowManagerKafkaImpl workflowManager;
    private final AutoCleanerHolder autoCleanerHolder;
    private final AtomicReference<SchedulerKafka> scheduler = new AtomicReference<>();

    volatile AtomicReference<CountDownLatch> debugLatch = new AtomicReference<>();

    public SchedulerSelectorKafka(WorkflowManagerKafkaImpl workflowManager, QueueFactory queueFactory,
            AutoCleanerHolder autoCleanerHolder) {
        this.workflowManager = workflowManager;
        this.autoCleanerHolder = autoCleanerHolder;
    }

    public WorkflowManagerState.State getState() {
        SchedulerKafka localScheduler = scheduler.get();
        return (localScheduler != null) ? localScheduler.getState() : WorkflowManagerState.State.LATENT;
    }

    public void start() {
        // Zkp implementation needs to do leader selection. Not needed in Kafka.
        // Only one or few workflow runners need to be present based on partitions of
        // the workflow topic. All extra consumers for a workflow queue exceeding
        // partitions will be idle till someone dies.

        log.info(workflowManager.getInstanceName() + " ready to act as scheduler");
        try {
            scheduler.set(new SchedulerKafka(workflowManager, autoCleanerHolder));
            new Thread(scheduler.get()).start();
        } finally {
            scheduler.set(null);

            CountDownLatch latch = debugLatch.getAndSet(null);
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    @Override
    public void close() {
    }

    @VisibleForTesting
    void debugValidateClosed() {
    }

}
