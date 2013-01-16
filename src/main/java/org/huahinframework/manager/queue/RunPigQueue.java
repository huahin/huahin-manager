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
package org.huahinframework.manager.queue;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.impl.util.PropertiesUtil;
import org.huahinframework.manager.util.JobUtils;

/**
 *
 */
public class RunPigQueue extends Thread {
    private static final Log log = LogFactory.getLog(RunPigQueue.class);

    private Properties properties = new Properties();
    private String queuePath;
    private Queue queue;

    /**
     * @param properties
     * @param queuePath
     * @param queue
     */
    public RunPigQueue(org.huahinframework.manager.Properties properties, String queuePath, Queue queue) {
        this.queuePath = queuePath;
        this.queue = queue;
        PropertiesUtil.loadDefaultProperties(this.properties);
        this.properties.putAll(ConfigurationUtil.toProperties(JobUtils.getJobConf(properties)));
    }

    /* (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        try {
            PigServer server = new PigServer(ExecType.MAPREDUCE, properties);
            server.registerQuery(queue.getScript());
            server.shutdown();
        } catch (Exception e) {
            queue.setMessage(e.toString());
            try {
                QueueUtils.registerQueue(queuePath, queue);
            } catch (IOException e1) {
                e1.printStackTrace();
                log.error(e1);
            }
            e.printStackTrace();
            log.error(e);
        }
    }
}
