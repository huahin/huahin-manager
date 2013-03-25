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
package org.huahinframework.manager;

/**
 *
 */
public class Properties {
    private static final String HUAHIN_HOME = System.getProperty("huahin.home");

    private String mapreduceJobTrackerAddress;
    private String fsDefaultFS;
    private String hiveserver;
    private int hiveserverVersion;
    private int jobQueueLimit;

    /**
     * @return the huahinHome
     */
    public String getHuahinHome() {
        return HUAHIN_HOME;
    }

    /**
     * @return the mapreduceJobTrackerAddress
     */
    public String getMapreduceJobtrackerAddress() {
        return mapreduceJobTrackerAddress;
    }

    /**
     * @param mapreduceJobTrackerAddress the mapreduceJobTrackerAddress to set
     */
    public void setMapreduceJobtrackerAddress(String mapreduceJobTrackerAddress) {
        this.mapreduceJobTrackerAddress = mapreduceJobTrackerAddress;
    }

    /**
     * @return the fsDefaultFS
     */
    public String getFsDefaultFS() {
        return fsDefaultFS;
    }

    /**
     * @param fsDefaultFS the fsDefaultFS to set
     */
    public void setFsDefaultFS(String fsDefaultFS) {
        this.fsDefaultFS = fsDefaultFS;
    }

    /**
     * @return the hiveserver
     */
    public String getHiveserver() {
        return hiveserver;
    }

    /**
     * @param hiveserver the hiveserver to set
     */
    public void setHiveserver(String hiveserver) {
        this.hiveserver = hiveserver;
    }

    /**
     * @return the hiveserverVersion
     */
    public int getHiveserverVersion() {
        return hiveserverVersion;
    }

    /**
     * @param hiveserverVersion the hiveserverVersion to set
     */
    public void setHiveserverVersion(int hiveserverVersion) {
        this.hiveserverVersion = hiveserverVersion;
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
