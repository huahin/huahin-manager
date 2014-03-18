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
import java.io.StringReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.text.MutableAttributeSet;
import javax.swing.text.html.HTML;
import javax.swing.text.html.HTML.Tag;
import javax.swing.text.html.HTMLEditorKit.ParserCallback;
import javax.swing.text.html.parser.ParserDelegator;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
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

    public static final String MAPREDUCE_JOBTRACKER_ADDRESS = "mapreduce.jobtracker.address";
    public static final String MAPRED_JOB_TRACKER = "mapred.job.tracker";
    public static final String FRAMEWORK_NAME = "mapreduce.framework.name";
    public static final String CLASSIC_FRAMEWORK_NAME = "classic";
    public static final String FS_DEFAULT_FS = "fs.defaultFS";

    public static final int ALL = -999;

    private static final String JOB_CONF_PATH = "/jobconf.jsp?jobid=";

    private static Calendar startTime = Calendar.getInstance();

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
        jobConf.set(MAPREDUCE_JOBTRACKER_ADDRESS, properties.getMapreduceJobtrackerAddress());
        jobConf.set(MAPRED_JOB_TRACKER, properties.getMapreduceJobtrackerAddress());
        jobConf.set(FRAMEWORK_NAME, CLASSIC_FRAMEWORK_NAME);
        jobConf.set(FS_DEFAULT_FS, properties.getFsDefaultFS());
        return jobConf;
    }

    /**
     * @param state
     * @param conf
     * @return {@link JSONArray}
     * @throws JSONException
     */
    @SuppressWarnings("unchecked")
    public static JSONArray getJobs(int state, JobConf conf) throws JSONException {
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
     */
    public static List<JSONObject> listJob(int state, JobConf conf) throws IOException {
        List<JSONObject> l = new ArrayList<JSONObject>();

        JobClient jobClient = new JobClient(conf);
        jobClient.setConf(conf);

        JobStatus[] jobStatuses = jobClient.getAllJobs();
        if (jobStatuses != null) {
            for (JobStatus jobStatus : jobStatuses) {
                if (state == ALL || state == jobStatus.getRunState()) {
                    Map<String, Object> m = getJob(jobClient, jobStatus);
                    if (m != null) {
                        l.add(new JSONObject(m));
                    }
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
    public static Map<String, Object> getJob(JobClient jobClient, JobStatus jobStatus) throws IOException {
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
        try {
            Counters jcnts = runningJob.getCounters();

            m.put(Response.MAPS_LAUNCHED, jcnts.getCounter(JobInProgress.Counter.TOTAL_LAUNCHED_MAPS));
            m.put(Response.REDUCES_LAUNCHED, jcnts.getCounter(JobInProgress.Counter.TOTAL_LAUNCHED_REDUCES));
        }
        catch (Exception e) {
            log.error("ERROR| Unable to get the job counters for JobID! " + jobStatus.getJobID().toString());
            e.printStackTrace();
            log.error(e);
        }

        m.put(Response.STATE, JobStatus.getJobRunState(jobStatus.getRunState()));
        m.put(Response.MAP_COMPLETE, jobStatus.mapProgress() * 100 + "%");
        m.put(Response.REDUCE_COMPLETE, jobStatus.reduceProgress() * 100 + "%");
        m.put(Response.SCHEDULEING_INFO, jobStatus.getSchedulingInfo());
        return m;
    }

    /**
     * @param trackingURL
     * @return job detail map
     * @throws HttpException
     * @throws IOException
     */
    public static Map<String, String> getJobDetail(final String trackingURL)
            throws HttpException, IOException {
        final Map<String, String> m = new HashMap<String, String>();
        HttpClient httpClient = new HttpClient();
        HttpMethod method = new GetMethod(trackingURL);

        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                                        new DefaultHttpMethodRetryHandler(3, false));

        int statusCode = httpClient.executeMethod(method);
        if (statusCode != 200) {
            method.releaseConnection();
            return null;
        }

        String responseBody = method.getResponseBodyAsString();

        ParserDelegator parser = new ParserDelegator();
        parser.parse(new StringReader(responseBody),
                     new ParserCallback() {
                         String name;
                         String value;
                         boolean b = false;
                         boolean afterB = false;

                         /**
                          * {@inheritDoc}
                          */
                         @Override
                         public void handleStartTag(Tag t, MutableAttributeSet a, int pos) {
                             if (t.equals(HTML.Tag.B)) {
                                 b = true;
                             }
                         }

                         /**
                          * {@inheritDoc}
                          */
                         @Override
                         public void handleText(char[] data, int pos) {
                             if (b) {
                                 name = new String(data).replace(":", "").replace(" ", "");
                             } else if (afterB) {
                                 value = new String(data).trim();
                                 if (!value.isEmpty()) {
                                     String key = nameMap.get(name);
                                     if (key != null) {
                                         m.put(key, value);
                                     }
                                 }

                                 afterB = false;
                                 name = null;
                                 value = null;
                             }
                         }

                         /**
                          * {@inheritDoc}
                          */
                         @Override
                         public void handleEndTag(Tag t, int pos) {
                             if (t.equals(HTML.Tag.B)) {
                                 b = false;
                                 afterB = true;
                             }
                         }
                     },
                     true);

        method.releaseConnection();
        return m;
    }

    /**
     * @param trackingURL job tracking URL
     * @param jobId job ID
     * @return job configuration map
     * @throws IOException
     * @throws HttpException
     */
    public static Map<String, String> getJobConfiguration(final String trackingURL, String jobId)
            throws HttpException, IOException {
        final Map<String, String> m = new HashMap<String, String>();
        HttpClient httpClient = new HttpClient();

        URI uri = URI.create(trackingURL);
        String jobconfURL = uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort() + JOB_CONF_PATH + jobId;

        HttpMethod method = new GetMethod(jobconfURL);
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                                        new DefaultHttpMethodRetryHandler(3, false));

        int statusCode = httpClient.executeMethod(method);
        if (statusCode != 200) {
            method.releaseConnection();
            return null;
        }

        String responseBody = method.getResponseBodyAsString();
        ParserDelegator parser = new ParserDelegator();
        parser.parse(new StringReader(responseBody),
                     new ParserCallback() {
                         String name;
                         String value;
                         boolean td = false;
                         boolean b = false;

                         /**
                          * {@inheritDoc}
                          */
                        @Override
                         public void handleStartTag(Tag t, MutableAttributeSet a, int pos) {
                            if (t.equals(HTML.Tag.B)) {
                                b = true;
                            } else if (t.equals(HTML.Tag.TD)) {
                                td = true;
                            }
                        }

                        /**
                         * {@inheritDoc}
                         */
                        @Override
                        public void handleText(char[] data, int pos) {
                            if (td && b) {
                                if (name != null && value == null) {
                                    m.put(name, "");
                                }
                                name = new String(data);
                            } else if (td) {
                                value = new String(data);
                            }
                        }

                        /**
                         * {@inheritDoc}
                         */
                        @Override
                        public void handleEndTag(Tag t, int pos) {
                            if (t.equals(HTML.Tag.B)) {
                                b = false;
                            } else if (t.equals(HTML.Tag.TD)) {
                                if (name != null && value != null) {
                                    m.put(name, value);
                                    name = null;
                                    value = null;
                                }
                                td = false;
                            }
                        }
                    },
                    true);
        method.releaseConnection();
        return m;
    }
}
