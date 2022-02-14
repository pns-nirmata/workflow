package com.nirmata.workflow.storage;

import java.util.Map;

public class RunRecord {
    private final byte[] runnableData;
    private final Map<String, byte[]> startedTasks;
    private final Map<String, byte[]> completedTasks;

    public RunRecord(byte[] runnableTaskData, Map<String, byte[]> startedTasks, Map<String, byte[]> completedTasks) {
        this.runnableData = runnableTaskData;
        this.startedTasks = startedTasks;
        this.completedTasks = completedTasks;
    }

    public byte[] getRunnableData() {
        return runnableData;
    }

    public Map<String, byte[]> getStartedTasks() {
        return startedTasks;
    }

    public Map<String, byte[]> getCompletedTasks() {
        return completedTasks;
    }

}
