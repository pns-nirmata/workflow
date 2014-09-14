package com.nirmata.workflow.queue.zookeeper;

import com.google.common.base.Preconditions;
import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.details.internalmodels.ExecutableTaskModel;
import com.nirmata.workflow.queue.Queue;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.utils.CloseableUtils;

public class ZooKeeperQueue implements Queue
{
    private final DistributedQueue<ExecutableTaskModel> queue;

    public ZooKeeperQueue(CuratorFramework curator, boolean idempotent)
    {
        curator = Preconditions.checkNotNull(curator, "curator cannot be null");
        String path = idempotent ? ZooKeeperConstants.IDEMPOTENT_TASKS_QUEUE_PATH : ZooKeeperConstants.NON_IDEMPOTENT_TASKS_QUEUE_PATH;
        QueueBuilder<ExecutableTaskModel> builder = QueueBuilder.builder(curator, null, new TaskQueueSerializer(), path);
        if ( idempotent )
        {
            builder = builder.lockPath(ZooKeeperConstants.IDEMPOTENT_TASKS_QUEUE_LOCK_PATH);
        }
        queue = builder.buildQueue();
    }

    @Override
    public void put(ExecutableTaskModel executableTask)
    {
        try
        {
            queue.put(executableTask);
        }
        catch ( Exception e )
        {
            // TODO log
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start()
    {
        try
        {
            queue.start();
        }
        catch ( Exception e )
        {
            // TODO log
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        CloseableUtils.closeQuietly(queue);
    }
}
