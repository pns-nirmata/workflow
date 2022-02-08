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
package com.nirmata.workflow.details.internalmodels;

import java.io.Serializable;
import java.util.Optional;

import com.nirmata.workflow.models.TaskExecutionResult;
import com.nirmata.workflow.models.TaskId;

public class WorkflowMessage implements Serializable {

    public enum MsgType {
        TASK, TASKRESULT
    };

    private final MsgType msgType;
    private final boolean isRetry;

    private final Optional<RunnableTask> runnableTask;
    private final Optional<TaskId> taskId;
    private final Optional<TaskExecutionResult> taskExecResult;

    public WorkflowMessage(RunnableTask rt) {
        this(rt, false);
    }

    public WorkflowMessage(RunnableTask rt, boolean isRetry) {
        this.msgType = MsgType.TASK;
        this.isRetry = isRetry;
        this.runnableTask = Optional.ofNullable(rt);
        this.taskId = Optional.ofNullable(null);
        this.taskExecResult = Optional.ofNullable(null);
    }

    public WorkflowMessage(TaskId taskId, TaskExecutionResult res) {
        this.msgType = MsgType.TASKRESULT;
        this.isRetry = false;
        this.runnableTask = Optional.ofNullable(null);
        this.taskId = Optional.ofNullable(taskId);
        this.taskExecResult = Optional.ofNullable(res);
    }

    public MsgType getMsgType() {
        return msgType;
    }

    public boolean isRetry() {
        return isRetry;
    }

    public Optional<RunnableTask> getRunnableTask() {
        return runnableTask;
    }

    public Optional<TaskId> getTaskId() {
        return taskId;
    }

    public Optional<TaskExecutionResult> getTaskExecResult() {
        return taskExecResult;
    }

}
