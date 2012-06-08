/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.huahin.manager.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.huahin.manager.Properties;
import org.huahin.manager.util.JobUtils;

/**
 *
 */
public class QueueManager implements Callable<Void> {
    private static final int POLLING_SECOND = (30 * 1000);

    private Properties properties;
    private JobConf jobConf;
    private int jobQueueLimit;
    private String queuePath;

    /**
     * @param properties
     */
    public QueueManager(Properties properties) {
        this.properties = properties;
        this.jobConf = JobUtils.getJobConf(properties);
        this.jobQueueLimit = properties.getJobQueueLimit();
        this.queuePath = QueueUtils.getQueuePath(properties.getHuahinHome());
    }

    /* (non-Javadoc)
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public Void call() throws Exception {
        try {
            List<RunnableFuture<Void>> threads = new ArrayList<RunnableFuture<Void>>();
            for (;;) {
                Map<String, Queue> runQueueMap = QueueUtils.readRemoveQueue(queuePath);
                for (Entry<String, Queue> entry : runQueueMap.entrySet()) {
                    QueueUtils.removeQueue(queuePath, entry.getValue());
                }

                Map<String, Queue> queueMap = QueueUtils.readQueue(queuePath);
                if (queueMap.isEmpty()) {
                    Thread.sleep(POLLING_SECOND);
                    continue;
                }

                int runnings = JobUtils.listJob(JobStatus.RUNNING, jobConf).size();
                int preps = JobUtils.listJob(JobStatus.PREP, jobConf).size();
                if (jobQueueLimit > 0 && (runnings + preps) >= jobQueueLimit) {
                    Thread.sleep(POLLING_SECOND);
                    continue;
                }

                List<RunnableFuture<Void>> removes = new ArrayList<RunnableFuture<Void>>();
                for (RunnableFuture<Void> t : threads) {
                    if (t.isDone()) {
                        removes.add(t);
                    }
                }
                threads.removeAll(removes);

                if (jobQueueLimit > 0 && threads.size() >= jobQueueLimit) {
                    Thread.sleep(POLLING_SECOND);
                    continue;
                }

                Queue queue = null;
                for (Queue q :queueMap.values()) {
                    queue = q;
                    break;
                }

                RunQueue runQueue = new RunQueue(JobUtils.getJobConf(properties), queuePath, queue);
                RunnableFuture<Void> runQueueThread = new FutureTask<Void>(runQueue);
                new Thread(runQueueThread).start();
                threads.add(runQueueThread);

                queue.setRun(true);
                QueueUtils.registerQueue(queuePath, queue);
            }
        } catch (Exception e) {
        }

        return null;
    }
}
