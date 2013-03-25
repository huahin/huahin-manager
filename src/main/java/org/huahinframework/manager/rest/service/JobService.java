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
package org.huahinframework.manager.rest.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.wink.common.internal.utils.MediaTypeUtils;
import org.apache.wink.common.model.multipart.InMultiPart;
import org.apache.wink.common.model.multipart.InPart;
import org.huahinframework.manager.queue.Queue;
import org.huahinframework.manager.queue.QueueUtils;
import org.huahinframework.manager.response.Response;
import org.huahinframework.manager.util.JobUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 */
@SuppressWarnings("deprecation")
@Path("/job")
public class JobService extends Service {
    private static final Log log = LogFactory.getLog(JobService.class);

    private static final String JOBID = "JobID";
    private static final String JOBNAME = "JobName";

    private static final String CONTENT_DISPOSITION = "Content-Disposition";
    private static final String FORM_DATA_NAME_JAR = "form-data; name=\"JAR\"";
    private static final String FORM_DATA_NAME_ARGUMENTS = "form-data; name=\"ARGUMENTS\"";

    private static final String JSON_CLASS = "class";
    private static final String JSON_SCRIPT = "script";
    private static final String JSON_ARGUMENTS = "arguments";

    private static final int JAR = 1;
    private static final int ARGUMENTS = 2;

    private static final Pattern fileNamePattern = Pattern.compile("^form-data; name=\"JAR\"; filename=\"(.*)\"$");

    /**
     * @return job {@link JSONArray}
     * @throws JSONException
     */
    @Path("/list")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONArray list() throws JSONException {
        return JobUtils.getJobs(JobUtils.ALL, getJobConf());
    }

    /**
     * @return job {@link JSONArray}
     * @throws JSONException
     */
    @Path("/list/failed")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONArray listFailed() throws JSONException {
        return JobUtils.getJobs(JobStatus.FAILED, getJobConf());
    }

    /**
     * @return job {@link JSONArray}
     * @throws JSONException
     */
    @Path("/list/killed")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONArray listKilled() throws JSONException {
        return JobUtils.getJobs(JobStatus.KILLED, getJobConf());
    }

    /**
     * @return job {@link JSONArray}
     * @throws JSONException
     */
    @Path("/list/prep")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONArray listPrep() throws JSONException {
        return JobUtils.getJobs(JobStatus.PREP, getJobConf());
    }

    /**
     * @return job {@link JSONArray}
     * @throws JSONException
     */
    @Path("/list/running")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONArray listRunning() throws JSONException {
        return JobUtils.getJobs(JobStatus.RUNNING, getJobConf());
    }

    /**
     * @return job {@link JSONArray}
     * @throws JSONException
     */
    @Path("/list/succeeded")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONArray listSucceeded() throws JSONException {
        return JobUtils.getJobs(JobStatus.SUCCEEDED, getJobConf());
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
            Map<String, Object> job = getStatus(jobId);
            if (job != null) {
                jsonObject = new JSONObject(job);
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
     * @param jobId
     * @return {@link JSONObject}
     * @throws JSONException
     */
    @Path("/detail/{" + JOBID + "}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONObject detail(@PathParam(JOBID) String jobId) throws JSONException {
        JSONObject jsonObject = null;
        try {
            final Map<String, Object> job = getStatus(jobId);
            if (job != null) {
                JobClient jobClient = new JobClient(getJobConf());
                RunningJob runningJob = jobClient.getJob(JobID.forName(jobId));
                if (runningJob != null) {
                    job.put(Response.JOB_FILE, runningJob.getJobFile());
                    String trackingURL = runningJob.getTrackingURL();
                    job.put(Response.TRACKING_URL, trackingURL);

                    Map<String, String> jobDetail = JobUtils.getJobDetail(trackingURL);
                    if (jobDetail != null) {
                        job.putAll(jobDetail);
                    }

                    Map<String, String> jobConf = JobUtils.getJobConfiguration(trackingURL, jobId);
                    if (jobConf != null) {
                        job.put(Response.CONFIGURATION, jobConf);
                    }
                }
                jsonObject = new JSONObject(job);
            }
        } catch (Exception e) {
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
     * @param inMP
     * @return {@link JSONObject}
     */
    @Path("/register")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaTypeUtils.MULTIPART_FORM_DATA)
    public JSONObject registerJob(InMultiPart inMP) {
        Map<String, String> status = new HashMap<String, String>();
        status.put(Response.STATUS, "accepted");

        try {
            Queue queue = new Queue();
            queue.setType(Queue.TYPE_JAR);
            synchronized (queue) {
                queue.setDate(new Date());
            }

            boolean jarFound = false;
            boolean argFound = false;
            File jarFile = File.createTempFile("huahin", ".jar", new File(getJarPath()));
            while (inMP.hasNext()) {
                InPart part = inMP.next();

                int type = 0;
                for (String s : part.getHeaders().get(CONTENT_DISPOSITION)) {
                    if (s.startsWith(FORM_DATA_NAME_JAR)) {
                        Matcher matcher = fileNamePattern.matcher(s);
                        if (matcher.matches() && matcher.groupCount() == 1) {
                            queue.setJarFileName(matcher.group(1));
                        }
                        type = JAR;
                        jarFound = true;
                        break;
                    } else if (s.startsWith(FORM_DATA_NAME_ARGUMENTS)) {
                        type = ARGUMENTS;
                        argFound = true;
                        break;
                    }
                }

                InputStream in = part.getInputStream();
                switch (type) {
                case JAR:
                    createJar(in, jarFile);
                    queue.setJar(jarFile.getPath());
                    break;
                case ARGUMENTS:
                    JSONObject argument = createJSON(in);
                    queue.setClazz(argument.getString(JSON_CLASS));

                    JSONArray array = argument.getJSONArray(JSON_ARGUMENTS);
                    String[] arguments = new String[array.length()];
                    for (int i = 0; i < array.length(); i++) {
                        arguments[i] = array.getString(i);
                    }
                    queue.setArguments(arguments);
                    break;
                }
            }

            if (!jarFound || !argFound) {
                jarFile.delete();
                status.put(Response.STATUS, "arguments error");
                return new JSONObject(status);
            }

            QueueUtils.registerQueue(getQueuePath(), queue);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
            status.put(Response.STATUS, e.getMessage());
        }

        return new JSONObject(status);
    }

    /**
     * @param inMP
     * @return {@link JSONObject}
     */
    @Path("/register/hive")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public JSONObject registerHive(InMultiPart inMP) {
        return registerScripts(Queue.TYPE_HIVE, inMP);
    }

    /**
     * @param inMP
     * @return {@link JSONObject}
     */
    @Path("/register/pig")
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public JSONObject registerPig(InMultiPart inMP) {
        return registerScripts(Queue.TYPE_PIG, inMP);
    }

    /**
     * @param type
     * @param inMP
     * @return JSONObject
     */
    private JSONObject registerScripts(int type, InMultiPart inMP) {
        Map<String, String> status = new HashMap<String, String>();
        status.put(Response.STATUS, "accepted");

        try {
            Queue queue = new Queue();
            queue.setType(type);
            synchronized (queue) {
                queue.setDate(new Date());
            }

            if (!inMP.hasNext()) {
                status.put(Response.STATUS, "arguments error");
                return new JSONObject(status);
            }

            InPart part = inMP.next();

            InputStream in = part.getInputStream();
            JSONObject argument = createJSON(in);
            queue.setScript(argument.getString(JSON_SCRIPT));

            QueueUtils.registerQueue(getQueuePath(), queue);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
            status.put(Response.STATUS, e.getMessage());
        }

        return new JSONObject(status);
    }

    /**
     * @param in
     * @param jarFile
     * @throws IOException
     */
    private void createJar(InputStream in, File jarFile) throws IOException {
        OutputStream out = new FileOutputStream(jarFile);

        byte[] buf = new byte[1024];
        int len = 0;
        while ((len = in.read(buf)) > 0) {
            out.write(buf, 0, len);
        }
        out.close();
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
     * @param jobId
     * @return {@link JSONObject}
     * @throws IOException
     */
    private Map<String, Object> getStatus(String jobId) throws IOException {
        Map<String, Object> job = null;

        JobClient jobClient = new JobClient(getJobConf());

        JobStatus[] jobStatuses = jobClient.getAllJobs();
        for (JobStatus jobStatus : jobStatuses) {
            if (jobStatus.getJobID().toString().equals(jobId)) {
                job = JobUtils.getJob(jobClient, jobStatus);
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
                break;
            }
        }

        return job;
    }
}
