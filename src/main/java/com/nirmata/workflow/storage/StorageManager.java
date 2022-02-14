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

import com.nirmata.workflow.models.RunId;
import com.nirmata.workflow.models.TaskId;

public interface StorageManager {

    byte[] getRunnable(RunId runId);

    List<String> getRunIds();

    Map<String, byte[]> getRuns();

    RunRecord getRunDetails(RunId runId);

    void createRun(RunId runId, byte[] runnableTaskBytes);

    void updateRun(RunId runId, byte[] runnableTaskBytes);

    byte[] getTaskExecutionResult(RunId runId, TaskId taskId);

    void saveTaskResult(RunId runId, TaskId taskId, byte[] taskResultData);

    byte[] getStartedTask(RunId runId, TaskId taskId);

    void setStartedTask(RunId runId, TaskId taskId, byte[] startedTaskData);

    boolean clean(RunId runId);
}
