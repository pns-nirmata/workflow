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

import java.time.LocalDateTime;
import java.util.Collections;
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

// In case workflow does not need functionality such as avoiding duplicate workflows with same name, 
// or retrying failed workflows, needing latest status of workflows, or storing run history
// the no-op implementation can be used
public class StorageManagerNoOpImpl implements StorageManager {

    @Override
    public Map<TaskId, TaskDetails> getTaskDetails(RunId runId) {
        return Collections.emptyMap();
    }

    @Override
    public void updateTaskProgress(RunId runId, TaskId taskId, int progress) {
        // No OP

    }

    @Override
    public Optional<TaskExecutionResult> getTaskExecutionResult(RunId runId, TaskId taskId) {
        return Optional.ofNullable(null);
    }

    @Override
    public boolean clean(RunId runId) {
        return true;
    }

    @Override
    public RunInfo getRunInfo(RunId runId) {
        return new RunInfo(runId, LocalDateTime.now());
    }

    @Override
    public List<RunId> getRunIds() {
        return Collections.emptyList();
    }

    @Override
    public List<RunInfo> getRunInfo() {
        return Collections.emptyList();
    }

    @Override
    public List<TaskInfo> getTaskInfo(RunId runId) {
        return Collections.emptyList();
    }

    @Override
    public void saveTaskResult(RunId runId, TaskExecutionResult result) {
        // No OP
    }

    @Override
    public void markComplete(RunId runId) {
        // No OP

    }

    @Override
    public void setStartedTask(RunId runId, TaskId taskId, StartedTask startedTask) {
        // No OP

    }

    @Override
    public RunDetails getRunDetails(RunId runId) {
        // No OP
        return null;
    }

}
