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

public interface StorageManager {

    Map<TaskId, TaskDetails> getTaskDetails(RunId runId);

    void updateTaskProgress(RunId runId, TaskId taskId, int progress);

    Optional<TaskExecutionResult> getTaskExecutionResult(RunId runId, TaskId taskId);

    boolean clean(RunId runId);

    RunInfo getRunInfo(RunId runId);

    List<RunId> getRunIds();

    List<RunInfo> getRunInfo();

    List<TaskInfo> getTaskInfo(RunId runId);

    void saveTaskResult(RunId runId, TaskExecutionResult result);

    void markComplete(RunId runId);

    void setStartedTask(RunId runId, TaskId taskId, StartedTask startedTask);

    RunDetails getRunDetails(RunId runId);

}
