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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.huahin.manager.response.Response;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 */
@Path("/job")
public class JobService extends Service {
    private static final Log log = LogFactory.getLog(JobService.class);

    private static final int ALL = -999;

    private static final String JOBID = "JobID";
    private static final String JOBNAME = "JobName";

    private Calendar startTime = Calendar.getInstance();

    /**
     * @return job {@link ResponseJobs}
     * @throws JSONException
     */
    @Path("/list")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONArray list() throws JSONException {
        return getJobs(ALL);
    }

    /**
     * @return job {@link JSONArray}
     * @throws JSONException
     */
    @Path("/list/failed")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONArray listFailed() throws JSONException {
        return getJobs(JobStatus.FAILED);
    }

    /**
     * @return job {@link JSONArray}
     * @throws JSONException
     */
    @Path("/list/killed")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONArray listKilled() throws JSONException {
        return getJobs(JobStatus.KILLED);
    }

    /**
     * @return job {@link JSONArray}
     * @throws JSONException
     */
    @Path("/list/prep")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONArray listPrep() throws JSONException {
        return getJobs(JobStatus.PREP);
    }

    /**
     * @return job {@link JSONArray}
     * @throws JSONException
     */
    @Path("/list/running")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONArray listRunning() throws JSONException {
        return getJobs(JobStatus.RUNNING);
    }

    /**
     * @return job {@link JSONArray}
     * @throws JSONException
     */
    @Path("/list/succeeded")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONArray listSucceeded() throws JSONException {
        return getJobs(JobStatus.SUCCEEDED);
    }

    /**
     * @return {@link JSONObject}
     * @throws JSONException
     */
    @Path("/status/{" + JOBID + "}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONObject status(@PathParam(JOBID) String jobId) throws JSONException {
        JSONObject jsonObject = null;
        try {
            JobClient jobClient = new JobClient(getJobConf());

            JobStatus[] jobStatuses = jobClient.getAllJobs();
            for (JobStatus jobStatus : jobStatuses) {
                if (jobStatus.getJobID().toString().equals(jobId)) {
                    Map<String, Object> job = getJob(jobClient, jobStatus);
                    RunningJob runningJob = jobClient.getJob(jobStatus.getJobID());
                    if (runningJob == null) {
                        break;
                    }

                    Map<String, Map<String, Long>> groups = new HashMap<String, Map<String,Long>>();
                    for (String s : runningJob.getCounters().getGroupNames()) {
                        Group group = runningJob.getCounters().getGroup(s);
                        Iterator<Counter> ite = group.iterator();

                        Map<String, Long> counters = new HashMap<String, Long>();
                        groups.put(group.getDisplayName(), counters);
                        while (ite.hasNext()) {
                            Counter counter = (Counter) ite.next();
                            counters.put(counter.getDisplayName(), counter.getValue());
                        }
                    }

                    job.put(Response.GROUPS, groups);
                    jsonObject = new JSONObject(job);
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e);
            Map<String, String> status = new HashMap<String, String>();
            status.put(Response.STATUS, e.getMessage());
            jsonObject = new JSONObject(status);
        }

        if (jsonObject == null) {
            Map<String, String> status = new HashMap<String, String>();
            status.put(Response.STATUS, "Could not find job " + jobId);
            jsonObject = new JSONObject(status);
        }

        return jsonObject;
    }

    /**
     * @return {@link JSONObject}
     */
    @Path("/kill/id/{" + JOBID + "}")
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    public JSONObject killJobId(@PathParam(JOBID) String jobId) {
        Map<String, String> status = new HashMap<String, String>();
        try {
            JobClient jobClient = new JobClient(getJobConf());

            JobStatus[] jobStatuses = jobClient.getAllJobs();
            for (JobStatus jobStatus : jobStatuses) {
                if (jobStatus.getJobID().toString().equals(jobId)) {
                    RunningJob runningJob = jobClient.getJob(jobStatus.getJobID());
                    if (runningJob == null) {
                        break;
                    }

                    runningJob.killJob();
                    status.put(Response.STATUS, "Killed job " + jobId);
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e);
            status.put(Response.STATUS, e.getMessage());
        }

        if (status.isEmpty()) {
            status.put(Response.STATUS, "Could not find job " + jobId);
        }

        return new JSONObject(status);
    }

    /**
     * @return {@link JSONObject}
     */
    @Path("/kill/name/{" + JOBNAME + "}")
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    public JSONObject killJobName(@PathParam(JOBNAME) String jobName) {
        Map<String, String> status = new HashMap<String, String>();
        try {
            JobClient jobClient = new JobClient(getJobConf());
            JobStatus[] jobStatuses = jobClient.getAllJobs();
            for (JobStatus jobStatus : jobStatuses) {
                RunningJob runningJob = jobClient.getJob(jobStatus.getJobID());
                if (runningJob == null) {
                    break;
                }

                if (runningJob.getJobName().equals(jobName)) {
                    runningJob.killJob();
                    status.put(Response.STATUS, "Killed job " + jobName);
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e);
            status.put(Response.STATUS, e.getMessage());
        }

        if (status.isEmpty()) {
            status.put(Response.STATUS, "Could not find job " + jobName);
        }

        return new JSONObject(status);
    }

    /**
     * @param state
     * @return {@link JSONArray}
     * @throws JSONException
     */
    @SuppressWarnings("unchecked")
    private JSONArray getJobs(int state) throws JSONException {
        JSONArray jsonArray = null;
        try {
            jsonArray = new JSONArray(listJob(state));
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
     * @return {@link List} of {@link JSONObject}
     * @throws IOException
     */
    private List<JSONObject> listJob(int state) throws IOException {
        List<JSONObject> l = new ArrayList<JSONObject>();

        JobClient jobClient = new JobClient(getJobConf());

        JobStatus[] jobStatuses = jobClient.getAllJobs();
        for (JobStatus jobStatus : jobStatuses) {
            if (state == ALL || state == jobStatus.getRunState()) {
                Map<String, Object> m = getJob(jobClient, jobStatus);
                if (m != null) {
                    l.add(new JSONObject(m));
                }
            }
        }

        return l;
    }

    /**
     * @param jobClient
     * @param jobStatus
     * @return JSON map
     * @throws IOException
     */
    private Map<String, Object> getJob(JobClient jobClient, JobStatus jobStatus) throws IOException {
        RunningJob runningJob = jobClient.getJob(jobStatus.getJobID());
        if (runningJob == null) {
            return null;
        }
        Map<String, Object> m = new HashMap<String, Object>();
        m.put(Response.JOBID, jobStatus.getJobID().toString());
        m.put(Response.PRIORITY, jobStatus.getJobPriority().name());
        m.put(Response.USER, jobStatus.getUsername());
        startTime.setTimeInMillis(jobStatus.getStartTime());
        m.put(Response.START_TIME, startTime.getTime().toString());
        m.put(Response.NAME, runningJob.getJobName());
        m.put(Response.STATE, JobStatus.getJobRunState(jobStatus.getRunState()));
        m.put(Response.MAP_COMPLETE, jobStatus.mapProgress() * 100 + "%");
        m.put(Response.REDUCE_COMPLETE, jobStatus.reduceProgress() * 100 + "%");
        m.put(Response.SCHEDULEING_INFO, jobStatus.getSchedulingInfo());
        return m;
    }
}
