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
package org.huahinframework.manager.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.httpclient.HttpException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.huahinframework.manager.Properties;
import org.huahinframework.manager.response.Response;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 */
public class JobUtils {
    private static final Log log = LogFactory.getLog(JobUtils.class);

    public static final String MAPREDUCE_JOBHISTORY_ADDRESS = "mapreduce.jobhistory.address";

    private static String memPattern   = "%dM";
    private static String UNAVAILABLE  = "N/A";

    private static Map<String, String> nameMap = new HashMap<String, String>();
    {
        nameMap.put("Finishedat", "finishedAt");
        nameMap.put("Finishedin", "finishedIn");
        nameMap.put("JobCleanup", "jobCleanup");
        nameMap.put("JobSetup", "jobSetup");
        nameMap.put("Status", "status");
        nameMap.put("SubmitHost", "submitHost");
        nameMap.put("SubmitHostAddress", "submitHostAddress");
    }

    /**
     * @param properties
     * @return
     */
    public static JobConf getJobConf(Properties properties) {
        JobConf jobConf = new JobConf();
        jobConf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
        jobConf.set(YarnConfiguration.RM_ADDRESS, properties.getRmAddress());
        jobConf.set(MAPREDUCE_JOBHISTORY_ADDRESS, properties.getMapreduceJobhistoryAddress());
        jobConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, properties.getFsDefaultFS());
        return jobConf;
    }

    /**
     * @param state
     * @param conf
     * @return {@link JSONArray}
     * @throws JSONException
     * @throws InterruptedException
     */
    @SuppressWarnings("unchecked")
    public static JSONArray getJobs(State state, JobConf conf)
            throws JSONException, InterruptedException {
        JSONArray jsonArray = null;
        try {
            jsonArray = new JSONArray(listJob(state, conf));
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e);
            Map<String, String> status = new HashMap<String, String>();
            status.put(Response.STATUS, e.getMessage());
            jsonArray = new JSONArray(Arrays.asList(status));
        }

        return jsonArray;
    }

    /**
     * @param state
     * @param conf
     * @return {@link List} of {@link JSONObject}
     * @throws IOException
     * @throws InterruptedException
     */
    public static List<JSONObject> listJob(State state, JobConf conf)
            throws IOException, InterruptedException {
        List<JSONObject> l = new ArrayList<JSONObject>();

        Cluster cluster = new Cluster(conf);
        for (JobStatus jobStatus : cluster.getAllJobStatuses()) {
            jobStatus.getState();
            if (state == null || state == jobStatus.getState()) {
                Map<String, Object> m = getJob(jobStatus);
                if (m != null) {
                    l.add(new JSONObject(m));
                }
            }
        }

        return l;
    }

    /**
     * @param jobStatus
     * @return JSON map
     * @throws IOException
     */
    public static Map<String, Object> getJob(JobStatus jobStatus) throws IOException {
        int numUsedSlots = jobStatus.getNumUsedSlots();
        int numReservedSlots = jobStatus.getNumReservedSlots();
        int usedMem = jobStatus.getUsedMem();
        int rsvdMem = jobStatus.getReservedMem();
        int neededMem = jobStatus.getNeededMem();

        Map<String, Object> m = new HashMap<String, Object>();
        m.put(Response.JOBID, jobStatus.getJobID().toString());
        m.put(Response.NAME, jobStatus.getJobName());
        m.put(Response.STATE, jobStatus.getState());

        Calendar startTime = Calendar.getInstance();
        startTime.setTimeInMillis(jobStatus.getStartTime());
        m.put(Response.START_TIME, startTime.getTime().toString());

        m.put(Response.USER, jobStatus.getUsername());
        m.put(Response.QUEUE, jobStatus.getQueue());
        m.put(Response.PRIORITY, jobStatus.getPriority().name());
        m.put(Response.USED_CONTAINERS, numUsedSlots < 0 ? UNAVAILABLE : numUsedSlots);
        m.put(Response.RSVD_CONTAINERS, numReservedSlots < 0 ? UNAVAILABLE : numReservedSlots);
        m.put(Response.USED_MEM, usedMem < 0 ? UNAVAILABLE : String.format(memPattern, usedMem));
        m.put(Response.RSVD_MEM, rsvdMem < 0 ? UNAVAILABLE : String.format(memPattern, rsvdMem));
        m.put(Response.NEEDED_MEM, neededMem < 0 ? UNAVAILABLE : String.format(memPattern, neededMem));
        m.put(Response.AM_INFO, jobStatus.getSchedulingInfo());
        return m;
    }

    /**
     * @param trackingURL job tracking URL
     * @param jobId job ID
     * @return job configuration map
     * @throws IOException
     * @throws HttpException
     * @throws DocumentException
     */
    public static Map<String, String> getJobConfiguration(final String jobFile, JobConf conf)
            throws HttpException, IOException {
        final Map<String, String> m = new HashMap<String, String>();

        URI uri = URI.create(jobFile);
        String path = uri.getPath();

        FileSystem fs = FileSystem.get(conf);
        FileStatus fstatus = fs.getFileStatus(new Path(path));
        if (fstatus == null) {
            return null;
        }

        InputStream is = fs.open(fstatus.getPath());
        Configuration jobConf = new JobConf(false);
        jobConf.addResource(is);

        Iterator<Entry<String, String>> ite = jobConf.iterator();
        while (ite.hasNext()) {
            Entry<String, String> entry = ite.next();
            m.put(entry.getKey(), entry.getValue());
        }
        is.close();

        return m;
    }
}
