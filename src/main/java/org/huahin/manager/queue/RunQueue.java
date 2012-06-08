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

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 */
public class RunQueue implements Callable<Void> {
    private static final Log log = LogFactory.getLog(RunQueue.class);

    private JobConf jobConf;
    private String queuePath;
    private Queue queue;

    /**
     * @param queue
     */
    public RunQueue(JobConf jobConf, String queuePath, Queue queue) {
        this.jobConf = jobConf;
        this.queuePath = queuePath;
        this.queue = queue;
    }

    /* (non-Javadoc)
     * @see java.util.concurrent.Callable#call()
     */
    @SuppressWarnings("unchecked")
    @Override
    public Void call() throws Exception {
        try {
            File jarFile = new File(queue.getJar());
            URL[] urls = { jarFile.toURI().toURL() };
            ClassLoader loader = URLClassLoader.newInstance(urls);
            Class<Tool> clazz = (Class<Tool>) loader.loadClass(queue.getClazz());

            ToolRunner.run(jobConf, clazz.newInstance(), queue.getArguments());
        } catch (Exception e) {
            queue.setMessage(e.toString());
            QueueUtils.registerQueue(queuePath, queue);
            e.printStackTrace();
            log.error(e);
        }

        return null;
    }
}
