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
import java.io.StringReader;
import java.net.URI;
import java.util.HashMap;
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
import org.apache.hadoop.mapred.JobConf;

/**
 *
 */
public abstract class Service {
    private static final String MAPRED_JOB_TRACKER = "mapred.job.tracker";

    private static final String JOB_CONF_PATH = "/jobconf.jsp?jobid=";

    private Map<String, String> nameMap = new HashMap<String, String>();
    {
        nameMap.put("Finishedat", "finishedAt");
        nameMap.put("Finishedin", "finishedIn");
        nameMap.put("JobCleanup", "jobCleanup");
        nameMap.put("JobSetup", "jobSetup");
        nameMap.put("Status", "status");
        nameMap.put("SubmitHost", "submitHost");
        nameMap.put("SubmitHostAddress", "submitHostAddress");
    }

    private String mapredJobTracker;

    /**
     * @return {@link JobConf}
     */
    protected JobConf getJobConf() {
        JobConf jobConf = new JobConf();
        jobConf.set(MAPRED_JOB_TRACKER, mapredJobTracker);
        return jobConf;
    }

    protected Map<String, String> getJobDetail(final String trackingURL)
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
    protected Map<String, String> getJobConfiguration(final String trackingURL, String jobId)
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

    /**
     * @param mapredJobTracker the mapredJobTracker to set
     */
    public void setMapredJobTracker(String mapredJobTracker) {
        this.mapredJobTracker = mapredJobTracker;
    }
}
