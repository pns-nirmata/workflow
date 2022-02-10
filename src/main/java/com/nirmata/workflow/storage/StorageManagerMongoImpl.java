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

package com.nirmata.workflow.storage;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.nirmata.workflow.admin.RunInfo;
import com.nirmata.workflow.admin.TaskDetails;
import com.nirmata.workflow.admin.TaskInfo;
import com.nirmata.workflow.details.internalmodels.RunDetails;
import com.nirmata.workflow.details.internalmodels.StartedTask;
import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;

// PNS TODO: Implement using the existing Zkp implementation as reference.
public class StorageManagerMongoImpl implements StorageManager {

    @Override
    public Map<TaskId, TaskDetails> getTaskDetails(RunId runId) {
        throw new UnsupportedOperationException("Mongo storage manager WIP");
    }

    @Override
    public void updateTaskProgress(RunId runId, TaskId taskId, int progress) {
        throw new UnsupportedOperationException("Mongo storage manager WIP");
    }

    @Override
    public Optional<TaskExecutionResult> getTaskExecutionResult(RunId runId, TaskId taskId) {
        throw new UnsupportedOperationException("Mongo storage manager WIP");
    }

    @Override
    public boolean clean(RunId runId) {
        throw new UnsupportedOperationException("Mongo storage manager WIP");
    }

    @Override
    public RunInfo getRunInfo(RunId runId) {
        // PNS TODO: Get run info from DB. Implement.
        // Better to call getRunInfo for a given Id
        // since DB supports it.
        throw new UnsupportedOperationException("Mongo storage manager WIP");
    }

    @Override
    public List<RunId> getRunIds() {
        // PNS TODO: Get run Ids from DB. Implement.
        // Zkp could not get particular ID, hence needed all.
        // Instead of getRunIds and iterating, one can call getRunId(id)
        throw new UnsupportedOperationException("Mongo storage manager WIP");
    }

    @Override
    public List<RunInfo> getRunInfo() {
        // PNS TODO: Get run info from DB. Implement.
        // Just like getRunIds above, better to call getRunInfo for a given Id
        // since DB supports it.
        throw new UnsupportedOperationException("Mongo storage manager WIP");
    }

    @Override
    public List<TaskInfo> getTaskInfo(RunId runId) {

        throw new UnsupportedOperationException("Mongo storage manager WIP");
    }

    @Override
    public void saveTaskResult(RunId runId, TaskExecutionResult result) {
        throw new UnsupportedOperationException("Mongo storage manager WIP");
    }

    @Override
    public void markComplete(RunId runId) {
        throw new UnsupportedOperationException("Mongo storage manager WIP");
    }

    @Override
    public void setStartedTask(RunId runId, TaskId taskId, StartedTask startedTask) {
        throw new UnsupportedOperationException("Mongo storage manager WIP");
    }

    @Override
    public RunDetails getRunDetails(RunId runId) {
        throw new UnsupportedOperationException("Mongo storage manager WIP");
    }
}
