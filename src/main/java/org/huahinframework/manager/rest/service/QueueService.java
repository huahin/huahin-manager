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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.huahinframework.manager.queue.Queue;
import org.huahinframework.manager.queue.QueueUtils;
import org.huahinframework.manager.response.Response;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 */
@Path("/queue")
public class QueueService extends Service {
    private static final Log log = LogFactory.getLog(QueueService.class);

    private static final String QUEUEID = "QueueID";

    /**
     * @return job {@link JSONArray}
     * @throws JSONException
     */
    @SuppressWarnings("unchecked")
    @Path("/list")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONArray list() throws JSONException {
        JSONArray jsonArray = null;

        try {
            List<JSONObject> l = new ArrayList<JSONObject>();
            Map<String, Queue> queueMap = QueueUtils.readQueue(getQueuePath());
            for (Entry<String, Queue> entry : queueMap.entrySet()) {
                Queue q = entry.getValue();
                Map<String, Object> m = new HashMap<String, Object>();
                m.put(Response.QUEUE_TYPE, Queue.toType(q.getType()));
                m.put(Response.QUEUE_ID, q.getId());
                m.put(Response.QUEUE_DATE, q.getDate().toString());
                m.put(Response.QUEUE_JAR, q.getJarFileName());
                m.put(Response.QUEUE_CLASS, q.getClazz());
                m.put(Response.QUEUE_SCRIPT, q.getScript());
                m.put(Response.QUEUE_ARGUMENTS, q.getArguments());

                l.add(new JSONObject(m));
            }

            jsonArray = new JSONArray(l);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
            Map<String, String> status = new HashMap<String, String>();
            status.put(Response.STATUS, e.getMessage());
            jsonArray = new JSONArray(Arrays.asList(status));
        }

        return jsonArray;
    }

    /**
     * @return {@link JSONArray}
     */
    @SuppressWarnings("unchecked")
    @Path("/statuses")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONArray statuses(@PathParam(QUEUEID) String queueId) {
        JSONArray jsonArray = null;

        try {
            List<JSONObject> l = new ArrayList<JSONObject>();
            Map<String, Queue> queueMap = QueueUtils.readRunQueue(getQueuePath());
            for (Entry<String, Queue> entry : queueMap.entrySet()) {
                Queue q = entry.getValue();
                Map<String, Object> m = new HashMap<String, Object>();
                m.put(Response.QUEUE_TYPE, Queue.toType(q.getType()));
                m.put(Response.QUEUE_ID, q.getId());
                m.put(Response.QUEUE_DATE, q.getDate().toString());
                m.put(Response.QUEUE_JAR, q.getJarFileName());
                m.put(Response.QUEUE_CLASS, q.getClazz());
                m.put(Response.QUEUE_SCRIPT, q.getScript());
                m.put(Response.QUEUE_ARGUMENTS, q.getArguments());
                m.put(Response.QUEUE_MESSAGE, q.getMessage());

                l.add(new JSONObject(m));
            }

            jsonArray = new JSONArray(l);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
            Map<String, String> status = new HashMap<String, String>();
            status.put(Response.STATUS, e.getMessage());
            jsonArray = new JSONArray(Arrays.asList(status));
        }

        return jsonArray;
    }

    /**
     * @return {@link JSONObject}
     */
    @Path("/kill/{" + QUEUEID + "}")
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    public JSONObject killJobId(@PathParam(QUEUEID) String queueId) {
        Map<String, String> status = new HashMap<String, String>();
        try {
            Map<String, Queue> queueMap = QueueUtils.readQueue(getQueuePath());
            for (Entry<String, Queue> entry : queueMap.entrySet()) {
                Queue q = entry.getValue();
                if (q.getId().equals(queueId)) {
                    QueueUtils.removeQueue(getQueuePath(), q);
                    status.put(Response.STATUS, "Killed queue " + queueId);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
            status.put(Response.STATUS, e.getMessage());
        }

        if (status.isEmpty()) {
            status.put(Response.STATUS, "Could not find queue " + queueId);
        }

        return new JSONObject(status);
    }
}
