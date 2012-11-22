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

    private String rmAddress;
    private String mapreduceJobhistoryAddress;
    private String fsDefaultFS;
    private int jobQueueLimit;

    /**
     * @return the huahinHome
     */
    public String getHuahinHome() {
        return HUAHIN_HOME;
    }

    /**
     * @return the rmAddress
     */
    public String getRmAddress() {
        return rmAddress;
    }

    /**
     * @param rmAddress the rmAddress to set
     */
    public void setRmAddress(String rmAddress) {
        this.rmAddress = rmAddress;
    }

    /**
     * @return the mapreduceJobhistoryAddress
     */
    public String getMapreduceJobhistoryAddress() {
        return mapreduceJobhistoryAddress;
    }

    /**
     * @param mapreduceJobhistoryAddress the mapreduceJobhistoryAddress to set
     */
    public void setMapreduceJobhistoryAddress(String mapreduceJobhistoryAddress) {
        this.mapreduceJobhistoryAddress = mapreduceJobhistoryAddress;
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
