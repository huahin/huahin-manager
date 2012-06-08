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
package org.huahin.manager;

/**
 *
 */
public class Properties {
    private static final String HUAHIN_HOME = System.getProperty("huahin.home");

    private String mapredJobTracker;
    private String fsDefaultName;
    private int jobQueueLimit;

    /**
     * @return the huahinHome
     */
    public String getHuahinHome() {
        return HUAHIN_HOME;
    }

    /**
     * @return the mapredJobTracker
     */
    public String getMapredJobTracker() {
        return mapredJobTracker;
    }

    /**
     * @param mapredJobTracker the mapredJobTracker to set
     */
    public void setMapredJobTracker(String mapredJobTracker) {
        this.mapredJobTracker = mapredJobTracker;
    }

    /**
     * @return the fsDefaultName
     */
    public String getFsDefaultName() {
        return fsDefaultName;
    }

    /**
     * @param fsDefaultName the fsDefaultName to set
     */
    public void setFsDefaultName(String fsDefaultName) {
        this.fsDefaultName = fsDefaultName;
    }

    /**
     * @return the jobQueueLimit
     */
    public int getJobQueueLimit() {
        return jobQueueLimit;
    }

    /**
     * @param jobQueueLimit the jobQueueLimit to set
     */
    public void setJobQueueLimit(int jobQueueLimit) {
        this.jobQueueLimit = jobQueueLimit;
    }
}
