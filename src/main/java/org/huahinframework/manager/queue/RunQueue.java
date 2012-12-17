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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 */
public class RunQueue extends Thread {
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
     * @see java.lang.Thread#run()
     */
    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        try {
            File jarFile = new File(queue.getJar());
            URL[] urls = { jarFile.toURI().toURL() };
            ClassLoader loader = URLClassLoader.newInstance(urls);
            Class<Tool> clazz = (Class<Tool>) loader.loadClass(queue.getClazz());

            if (!(clazz.newInstance() instanceof Tool)) {
                queue.setMessage("this jar not supported. this class dose not instance of Tool.class.");
                QueueUtils.registerQueue(queuePath, queue);
                return;
            }

            log.info("job start: " + clazz.getName());
            ToolRunner.run(jobConf, clazz.newInstance(), queue.getArguments());
            log.info("job end: " + clazz.getName());
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
