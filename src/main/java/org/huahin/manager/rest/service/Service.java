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
package org.huahin.manager.rest.service;

import org.apache.hadoop.mapred.JobConf;
import org.huahin.manager.Properties;
import org.huahin.manager.queue.QueueUtils;
import org.huahin.manager.util.JobUtils;

/**
 *
 */
public abstract class Service {
    private Properties properties;
    private String queuePath;
    private String jarPath;

    /**
     * @return {@link JobConf}
     */
    protected JobConf getJobConf() {
        return JobUtils.getJobConf(properties);
    }

    /**
     * @return queue path
     */
    public String getQueuePath() {
        return queuePath;
    }

    /**
     * @return jar path
     */
    public String getJarPath() {
        return jarPath;
    }

    /**
     * @param properties the properties to set
     */
    public void setProperties(Properties properties) {
        this.properties = properties;
        this.queuePath = QueueUtils.getQueuePath(properties.getHuahinHome());
        this.jarPath = properties.getHuahinHome() + "/queue/jar/";
    }
}
