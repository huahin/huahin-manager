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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Times;
import org.huahinframework.manager.response.Response;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 */
@Path("/application")
public class ApplicationService extends Service {
    private static final Log log = LogFactory.getLog(ApplicationService.class);

    private static final String APPLICATION_ID = "ApplicationID";

    private ClientRMProtocol applicationsManager;
    private RecordFactory recordFactory;

    @Path("/list")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONObject list() {
        JSONObject jsonObject = new JSONObject();

        try {
            GetAllApplicationsRequest request = recordFactory.newRecordInstance(GetAllApplicationsRequest.class);
            GetAllApplicationsResponse response = applicationsManager.getAllApplications(request);

            JSONObject appObject = new JSONObject();
            List<JSONObject> apps = new ArrayList<JSONObject>();
            for (ApplicationReport ar : response.getApplicationList()) {
                JSONObject app = new JSONObject();
                app.put(Response.ID, ar.getApplicationId().toString());
                app.put(Response.USER, ar.getUser());
                app.put(Response.NAME, ar.getName());
                app.put(Response.QUEUE, ar.getQueue());
                YarnApplicationState state = ar.getYarnApplicationState();
                app.put(Response.STATE, state);
                app.put(Response.FINAL_STATUS, ar.getFinalApplicationStatus().name());
                String trackingUrl = ar.getTrackingUrl();
                boolean trackingUrlIsNotReady =
                        trackingUrl == null || trackingUrl.isEmpty() ||
                        YarnApplicationState.NEW == state || YarnApplicationState.SUBMITTED == state ||
                        YarnApplicationState.ACCEPTED == state;
                String trackingUI =
                        trackingUrlIsNotReady ?
                                "UNASSIGNED" : (ar.getFinishTime() == 0 ? "ApplicationMaster" : "History");
                app.put(Response.TRACKING_UI, trackingUI);
                app.put(Response.TRACKING_URL, trackingUrl);
                app.put(Response.DIAGNOSTICS, ar.getDiagnostics());
                app.put(Response.START_TIME, new Date(ar.getStartTime()));
                app.put(Response.FINISHED_TIME, ar.getFinishTime() == 0 ? "" : new Date(ar.getFinishTime()));
                app.put(Response.ELAPSED_TIME, (Times.elapsed(ar.getStartTime(), ar.getFinishTime()) / 1000) + "sec");
                apps.add(app);
            }

            appObject.put(Response.APP, new JSONArray(apps));
            jsonObject.put(Response.APPS, appObject);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
            Map<String, String> status = new HashMap<String, String>();
            status.put(Response.STATUS, e.getMessage());
            jsonObject = new JSONObject(status);
        }

        return jsonObject;
    }

    @Path("/get/cluster")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JSONObject getCluster() {
        JSONObject jsonObject = new JSONObject();

        try {
            GetClusterMetricsRequest metricsRequest = recordFactory.newRecordInstance(GetClusterMetricsRequest.class);
            GetClusterMetricsResponse metricsResponse = applicationsManager.getClusterMetrics(metricsRequest);

            jsonObject.put(Response.NUM_NODE_MANAGERS, metricsResponse.getClusterMetrics().getNumNodeManagers());

            GetClusterNodesRequest nodeRequest = recordFactory.newRecordInstance(GetClusterNodesRequest.class);
            GetClusterNodesResponse nodeResponse = applicationsManager.getClusterNodes(nodeRequest);

            List<JSONObject> reports = new ArrayList<JSONObject>();
            for (NodeReport report : nodeResponse.getNodeReports()) {
                JSONObject nr = new JSONObject();
                nr.put(Response.HTTP_ADDRESS, report.getHttpAddress());
                nr.put(Response.NUM_CONTAINERS, report.getNumContainers());
                nr.put(Response.RACK_NAME, report.getRackName());
                nr.put(Response.CAPABILITY, report.getCapability().getMemory());
                nr.put(Response.HEALTH_REPORT, report.getNodeHealthStatus().getHealthReport());
                nr.put(Response.IS_NODE_HEALTHY, report.getNodeHealthStatus().getIsNodeHealthy());
                nr.put(Response.LAST_HEALTH_REPORT_TIME,
                       new Date(report.getNodeHealthStatus().getLastHealthReportTime()));
                nr.put(Response.NODE_ID, report.getNodeId());
                nr.put(Response.NODE_STATE, report.getNodeState());
                nr.put(Response.NODE_STATE, report.getNodeState());
                nr.put(Response.USED, report.getUsed());
                reports.add(nr);
            }

            jsonObject.put(Response.NODES, reports);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
            Map<String, String> status = new HashMap<String, String>();
            status.put(Response.STATUS, e.getMessage());
            jsonObject = new JSONObject(status);
        }

        return jsonObject;
    }

    @Path("/kill/{" + APPLICATION_ID + "}")
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    public JSONObject kill(@PathParam(APPLICATION_ID) String applicationId) {
        Map<String, String> status = new HashMap<String, String>();
        try {
            GetAllApplicationsRequest getRequest = recordFactory.newRecordInstance(GetAllApplicationsRequest.class);
            GetAllApplicationsResponse getResponse = applicationsManager.getAllApplications(getRequest);
            for (ApplicationReport ar : getResponse.getApplicationList()) {
                if (ar.getApplicationId().toString().equals(applicationId)) {
                    KillApplicationRequest killRequest = recordFactory.newRecordInstance(KillApplicationRequest.class);
                    killRequest.setApplicationId(ar.getApplicationId());
                    applicationsManager.forceKillApplication(killRequest);

                    status.put(Response.STATUS, "Killed application " + applicationId);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
            status.put(Response.STATUS, e.getMessage());
        }

        if (status.isEmpty()) {
            status.put(Response.STATUS, "Could not find application " + applicationId);
        }

        return new JSONObject(status);
    }

    /**
     * init
     */
    public void init() {
        YarnRPC rpc = YarnRPC.create(getJobConf());
        YarnConfiguration yarnConf = new YarnConfiguration(getJobConf());
        InetSocketAddress rmAddress =
                NetUtils.createSocketAddr(yarnConf.get(YarnConfiguration.RM_ADDRESS,
                                                       YarnConfiguration.DEFAULT_RM_ADDRESS));
        applicationsManager =
                ((ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class, rmAddress, yarnConf));
        recordFactory = RecordFactoryProvider.getRecordFactory(yarnConf);
    }
}
